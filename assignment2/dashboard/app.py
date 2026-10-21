#!/usr/bin/env python3
"""Map the simulated EV charging stations with live prices, occupancy and charging curves.

The page polls the A2 DuckDB on a timer. With ``--subscribe`` the same process
also runs B's MQTT subscriber, so one process owns database writes and the
dashboard's reads are serialised with them through a shared lock.
"""

from __future__ import annotations

import argparse
import sys
import threading
from argparse import Namespace
from pathlib import Path
from typing import Any

if not __package__:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import dash
import dash_leaflet as dl
import duckdb
import plotly.graph_objects as go
from dash import ALL, Dash, Input, Output, Patch, State, dcc, html
from plotly.subplots import make_subplots

from assignment2.dashboard.queries import DashboardData, load_simulated_stations


A2_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DATABASE = A2_ROOT / "data/final/project_a2.duckdb"
DEFAULT_STATIONS = A2_ROOT / "data/generated/sample_20/stations.csv"

NO_PRICE_TEXT = "No price received yet"

# Reference data-viz palette: muted grey for "no data", one blue for "has a
# price", and an ordinal blue ramp (light -> dark) for occupancy.
INK = "#0b0b0b"
INK_SECONDARY = "#52514e"
INK_MUTED = "#898781"
GRID = "#e1e0d9"
AXIS = "#c3c2b7"
SURFACE = "#fcfcfb"
SERIES = "#2a78d6"
PRICE_COLOURS = {"none": INK_MUTED, "priced": SERIES}
OCCUPANCY_COLOURS = {"free": "#86b6ef", "partial": "#2a78d6", "full": "#104281"}

PAGE_CSS = """
html, body { height: 100%; }
body { margin: 0; font-family: system-ui, -apple-system, "Segoe UI", sans-serif; color: #0b0b0b; background: #f9f9f7; }
.header { padding: 10px 16px; border-bottom: 1px solid #e1e0d9; background: #fcfcfb; }
.header h1 { font-size: 18px; margin: 0 0 4px; }
.status { font-size: 13px; color: #52514e; }
.status .clock { color: #0b0b0b; font-weight: 600; }
.status .error { color: #d03b3b; }
.app { display: flex; flex-direction: column; height: 100vh; }
.main { display: flex; flex: 1 1 auto; min-height: 0; }
.map-wrap { flex: 1 1 auto; min-width: 0; }
.side { width: 440px; max-width: 48vw; overflow-y: auto; padding: 12px 16px; border-left: 1px solid #e1e0d9; font-size: 13px; background: #fcfcfb; }
.side h2 { font-size: 15px; margin: 18px 0 6px; }
.controls { display: flex; flex-wrap: wrap; gap: 4px 16px; align-items: center; }
.controls label { margin-right: 10px; }
.legend { margin: 6px 0 0; color: #52514e; }
.legend span { display: inline-block; margin-right: 12px; }
.dot { display: inline-block; width: 10px; height: 10px; border-radius: 50%; margin-right: 4px; vertical-align: middle; }
.ring { display: inline-block; width: 10px; height: 10px; border-radius: 50%; margin-right: 4px; vertical-align: middle; border: 2px solid #0b0b0b; box-sizing: border-box; }
table.details { border-collapse: collapse; width: 100%; }
table.details th { text-align: left; font-weight: 600; padding: 2px 8px 2px 0; vertical-align: top; white-space: nowrap; }
table.details td { padding: 2px 0; word-break: break-word; }
table.events { border-collapse: collapse; width: 100%; font-variant-numeric: tabular-nums; }
table.events th, table.events td { text-align: left; padding: 2px 6px 2px 0; border-bottom: 1px solid #f0efec; }
.tiles { display: flex; gap: 8px; margin: 8px 0; }
.tile { flex: 1; border: 1px solid #e1e0d9; border-radius: 6px; padding: 6px 8px; background: #ffffff; }
.tile .label { color: #52514e; font-size: 12px; }
.tile .value { font-size: 20px; font-weight: 600; }
.tile .sub { color: #898781; font-size: 11px; }
.missing { color: #a50e0e; font-style: italic; }
.muted { color: #52514e; }
.price { font-size: 18px; font-weight: 600; }
.leaflet-tooltip.value-label { padding: 0 4px; font-size: 11px; font-weight: 600; color: #0b0b0b; background: #fcfcfb; border: 1px solid #c3c2b7; border-radius: 4px; box-shadow: none; font-variant-numeric: tabular-nums; }
.leaflet-tooltip.value-label::before { display: none; }
.popup-curve-caption { font-size: 12px; color: #52514e; margin-top: 8px; }
details summary { cursor: pointer; color: #52514e; margin: 4px 0; }
@media (max-width: 720px) {
  .app { height: auto; }
  .main { flex-direction: column; }
  .map-wrap { height: 60vh; }
  .side { width: auto; max-width: none; border-left: none; border-top: 1px solid #e1e0d9; }
}
"""


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--database", type=Path, default=DEFAULT_DATABASE, help="A2 DuckDB file.")
    parser.add_argument(
        "--stations",
        type=Path,
        default=DEFAULT_STATIONS,
        help="Generator stations.csv defining which stations are simulated.",
    )
    parser.add_argument("--refresh-seconds", type=float, default=1.0, help="Database polling interval.")
    parser.add_argument(
        "--highlight-seconds",
        type=float,
        default=5.0,
        help="Ring stations whose last price or session event arrived within this many seconds.",
    )
    parser.add_argument("--host", default="127.0.0.1", help="Dashboard web server host.")
    parser.add_argument("--port", type=int, default=8050, help="Dashboard web server port.")
    parser.add_argument(
        "--subscribe",
        action="store_true",
        help="Run B's MQTT subscriber in this process and store received events.",
    )
    parser.add_argument("--mqtt-host", default="localhost", help="MQTT Broker host.")
    parser.add_argument("--mqtt-port", type=int, default=1883, help="MQTT Broker port.")
    parser.add_argument("--mqtt-topic", default="evchargers/+/+", help="MQTT topic filter.")
    parser.add_argument(
        "--mqtt-client-id",
        default="comp5339-dashboard-subscriber",
        help="MQTT client identifier (differs from the standalone subscriber).",
    )
    parser.add_argument("--username", help="Optional MQTT username.")
    parser.add_argument("--password", help="Optional MQTT password.")
    parser.add_argument(
        "--received-log",
        type=Path,
        help="Optional JSONL file for events accepted by the in-process subscriber.",
    )
    args = parser.parse_args()
    if args.refresh_seconds <= 0:
        parser.error("--refresh-seconds must be positive")
    if args.highlight_seconds < 0:
        parser.error("--highlight-seconds cannot be negative")
    if bool(args.username) != bool(args.password):
        parser.error("--username and --password must be supplied together")
    return args


def start_subscriber(args: argparse.Namespace, lock: threading.Lock) -> Any:
    """Start B's subscriber in a background thread, sharing ``lock`` for writes."""

    from assignment2.mqtt.subscriber import PriceSubscriber

    subscriber_args = Namespace(
        host=args.mqtt_host,
        port=args.mqtt_port,
        topic=args.mqtt_topic,
        client_id=args.mqtt_client_id,
        username=args.username,
        password=args.password,
        output=args.received_log,
        database=args.database,
        max_messages=0,
    )
    with lock:
        subscriber = PriceSubscriber(subscriber_args)

    store_event = subscriber.store_event

    def locked_store_event(database_path: Path, event: dict[str, Any]) -> str:
        with lock:
            return store_event(database_path, event)

    subscriber.store_event = locked_store_event
    threading.Thread(target=subscriber.run, name="event-subscriber", daemon=True).start()
    return subscriber


# ---------------------------------------------------------------- formatting


def _time(value: str | None) -> str | None:
    return None if value is None else value.replace("T", " ")


def _clock(value: str | None) -> str:
    """HH:MM:SS part of an ISO timestamp."""
    return "" if value is None else value[11:19]


def _value(value: Any, unit: str = "") -> Any:
    if value is None:
        return html.Span("Missing", className="missing")
    return f"{value}{unit}"


def _price_text(latest: dict[str, Any] | None) -> str:
    if latest is None:
        return NO_PRICE_TEXT
    return f"${latest['price_per_kwh']:.2f}/kWh"


def _plugs(station: dict[str, Any]) -> int | None:
    """Connector count used for occupancy: the simulated count, else A1 plugs."""
    return station["simulated_connectors"] or station["plugs"]


def _occupancy_text(station: dict[str, Any]) -> str:
    plugs = _plugs(station)
    of = f" of {plugs}" if plugs else ""
    return f"{station['occupied_connectors']}{of} plugs in use"


def _occupancy_state(station: dict[str, Any]) -> str:
    occupied = station["occupied_connectors"]
    plugs = _plugs(station)
    if occupied == 0:
        return "free"
    if plugs and occupied >= plugs:
        return "full"
    return "partial"


def _is_recent(station: dict[str, Any], highlight_seconds: float) -> bool:
    age = station["activity_age_seconds"]
    return age is not None and age <= highlight_seconds


def _name_cell(station: dict[str, Any]) -> Any:
    if station["name"] is not None:
        return station["name"]
    label = station["generator_label"]
    note = f" (generator label: {label})" if label else ""
    return html.Span([html.Span("Missing in A1", className="missing"), note])


def _plug_types_cell(station: dict[str, Any]) -> Any:
    """Connector types from A1's Open Charge Map match; A1's own field is only AC/DC."""
    charger_type = f" · A1 charger type: {station['charger_type']}" if station["charger_type"] else ""
    if station["plug_types"]:
        return f"{station['plug_types']}{charger_type}"
    return html.Span([html.Span("Missing", className="missing"), charger_type])


def _title(station: dict[str, Any]) -> str:
    return station["name"] or station["address"] or station["generator_label"] or station["station_id"]


# ---------------------------------------------------------------- station views


def details_table(station: dict[str, Any]) -> html.Table:
    """A1 details, latest received price and the current/historical metrics."""

    latest = station["latest"]
    if latest is None:
        price_cells = [("Latest price", html.Span(NO_PRICE_TEXT, className="missing"))]
    else:
        price_cells = [
            ("Latest price", html.Span(_price_text(latest), className="price")),
            ("Price event time", _time(latest["event_time"])),
        ]
    rows = [
        ("Name", _name_cell(station)),
        ("Address", _value(station["address"])),
        ("Operator", _value(station["operator"])),
        ("Plug types", _plug_types_cell(station)),
        ("Plugs (A1)", _value(station["plugs"])),
        ("Power (A1)", _value(station["power_kw"], " kW")),
        ("Charger rating", _value(station["charger_rating"])),
        ("Site type (simulated)", _value(station["site_type"])),
        *price_cells,
        ("In use now", f"{_occupancy_text(station)} · {station['active_sessions']} active sessions"),
        (
            "Completed sessions",
            f"{station['completed_sessions']} · {station['completed_energy_kwh']:.1f} kWh delivered",
        ),
    ]
    return html.Table(
        [html.Tr([html.Th(label), html.Td(value)]) for label, value in rows],
        className="details",
    )


def popup_table(station: dict[str, Any]) -> html.Table:
    """The fields the popup must show, kept short so the live curve fits below."""
    latest = station["latest"]
    if latest is None:
        price = html.Span(NO_PRICE_TEXT, className="missing")
    else:
        price = html.Span(
            [html.Span(_price_text(latest), className="price"), f" at {_clock(latest['event_time'])}"]
        )
    rows = [
        ("Name", _name_cell(station)),
        ("Address", _value(station["address"])),
        ("Operator", _value(station["operator"])),
        ("Plug types", _plug_types_cell(station)),
        ("Plugs / power", f"{_value(station['plugs'])} plugs · {_value(station['power_kw'], ' kW')}"
         if station["plugs"] is not None and station["power_kw"] is not None
         else _value(None)),
        ("Latest price", price),
        ("In use now", f"{_occupancy_text(station)} · {station['active_sessions']} active sessions"),
        (
            "Completed sessions",
            f"{station['completed_sessions']} · {station['completed_energy_kwh']:.1f} kWh delivered",
        ),
    ]
    return html.Table(
        [html.Tr([html.Th(label), html.Td(value)]) for label, value in rows],
        className="details",
    )


def popup_content(station: dict[str, Any]) -> Any:
    return html.Div(
        [
            popup_table(station),
            html.Div(
                "Counts cover events received since the replay started.",
                className="muted",
                style={"fontSize": "11px", "marginTop": "4px"},
            ),
        ]
    )


def metric_tiles(station: dict[str, Any]) -> html.Div:
    plugs = _plugs(station)
    return html.Div(
        [
            html.Div(
                [
                    html.Div("In use now", className="label"),
                    html.Div(
                        f"{station['occupied_connectors']}" + (f" / {plugs}" if plugs else ""),
                        className="value",
                    ),
                    html.Div(f"plugs · {station['active_sessions']} active sessions", className="sub"),
                ],
                className="tile",
            ),
            html.Div(
                [
                    html.Div("Completed sessions", className="label"),
                    html.Div(str(station["completed_sessions"]), className="value"),
                    html.Div(f"{station['completed_energy_kwh']:.1f} kWh delivered", className="sub"),
                ],
                className="tile",
            ),
            html.Div(
                [
                    html.Div("Latest price", className="label"),
                    html.Div(
                        f"${station['latest']['price_per_kwh']:.2f}" if station["latest"] else "—",
                        className="value",
                    ),
                    html.Div(
                        "per kWh" if station["latest"] else NO_PRICE_TEXT.lower(),
                        className="sub",
                    ),
                ],
                className="tile",
            ),
        ],
        className="tiles",
    )


def marker_style(
    station: dict[str, Any], mode: str, highlight_seconds: float
) -> dict[str, Any]:
    recent = _is_recent(station, highlight_seconds)
    if mode == "occupancy":
        fill = OCCUPANCY_COLOURS[_occupancy_state(station)]
    else:
        fill = PRICE_COLOURS["priced" if station["latest"] else "none"]
    # dash-leaflet re-applies colours only when pathOptions changes, so style
    # lives there; radius is updated separately.
    return {
        "pathOptions": {
            "fillColor": fill,
            "fillOpacity": 0.9,
            # Recent activity gets a dark ring; otherwise a surface ring separates overlaps.
            "color": INK if recent else "#ffffff",
            "weight": 3 if recent else 2,
        },
        "radius": 12 if recent else 8,
    }


def marker_label(station: dict[str, Any], mode: str) -> str:
    """The value shown on the marker itself: price, or plugs in use."""
    if mode == "occupancy":
        plugs = _plugs(station)
        return f"{station['occupied_connectors']}/{plugs}" if plugs else str(station["occupied_connectors"])
    latest = station["latest"]
    return f"${latest['price_per_kwh']:.2f}" if latest else "no price"


def station_signature(station: dict[str, Any], mode: str, highlight_seconds: float) -> list[Any]:
    """Marker and popup content only change when this changes."""
    return [
        mode,
        station["latest"]["event_id"] if station["latest"] else None,
        station["active_sessions"],
        station["occupied_connectors"],
        station["completed_sessions"],
        round(station["completed_energy_kwh"], 3),
        _is_recent(station, highlight_seconds),
    ]


def build_marker(station: dict[str, Any], mode: str, highlight_seconds: float) -> Any:
    station_id = station["station_id"]
    return dl.CircleMarker(
        id={"type": "station", "id": station_id},
        center=[station["lat"], station["lon"]],
        **marker_style(station, mode, highlight_seconds),
        children=[
            # A permanent label stays open, so its inner text can be updated in place.
            dl.Tooltip(
                html.Span(marker_label(station, mode), id={"type": "label", "id": station_id}),
                permanent=True,
                direction="right",
                offset={"x": 8, "y": 0},
                className="value-label",
            ),
        ],
    )


def build_markers(
    snapshot: dict[str, Any], mode: str, highlight_seconds: float
) -> list[Any]:
    """Markers for stations that have received at least one message."""
    return [
        build_marker(station, mode, highlight_seconds)
        for station in snapshot["stations"]
        if station["has_events"]
    ]


def station_popup(station: dict[str, Any], nonce: Any) -> Any:
    """A fresh popup for each click.

    dash-leaflet loses a marker-bound popup's content once it has been closed
    and reopened, so one standalone popup is mounted per click (new id) and
    only its inner body is refreshed afterwards.
    """
    station_id = station["station_id"]
    return dl.Popup(
        [
            html.Div(popup_content(station), id={"type": "popup-body", "id": station_id}),
            html.Div(id={"type": "popup-caption", "id": station_id}, className="popup-curve-caption"),
            # Space for the curve is reserved at open time, so Leaflet's auto-pan
            # moves the whole popup into view before the first refresh fills it.
            dcc.Graph(
                id={"type": "popup-curve", "id": station_id},
                figure=go.Figure(),
                config={"displayModeBar": False},
                style={"display": "block" if station["active_sessions"] else "none", "height": "170px"},
            ),
        ],
        id={"type": "popup", "id": station_id, "click": nonce},
        position=[station["lat"], station["lon"]],
        maxWidth=400,
        minWidth=340,
    )


def legend(mode: str, highlight_seconds: float) -> html.Div:
    if mode == "occupancy":
        items = [
            ("Free", OCCUPANCY_COLOURS["free"]),
            ("Partly in use", OCCUPANCY_COLOURS["partial"]),
            ("All plugs in use", OCCUPANCY_COLOURS["full"]),
        ]
    else:
        items = [("No price yet", PRICE_COLOURS["none"]), ("Has a price", PRICE_COLOURS["priced"])]
    return html.Div(
        [html.Span([html.I(className="dot", style={"background": colour}), label]) for label, colour in items]
        + [html.Span([html.I(className="ring"), f"Event in last {highlight_seconds:g}s"])],
        className="legend",
    )


# ---------------------------------------------------------------- charts


def _base_layout(fig: go.Figure, height: int, revision: str) -> None:
    fig.update_layout(
        height=height,
        margin={"l": 56, "r": 12, "t": 28, "b": 36},
        paper_bgcolor=SURFACE,
        plot_bgcolor=SURFACE,
        font={"family": 'system-ui, -apple-system, "Segoe UI", sans-serif', "size": 12, "color": INK_SECONDARY},
        showlegend=False,
        hovermode="x unified",
        uirevision=revision,
    )
    fig.update_xaxes(
        showgrid=False, linecolor=AXIS, tickcolor=AXIS, tickfont={"color": INK_MUTED},
        showspikes=True, spikemode="across", spikethickness=1, spikecolor=AXIS, spikedash="solid",
    )
    fig.update_yaxes(gridcolor=GRID, zeroline=False, linecolor=AXIS, tickfont={"color": INK_MUTED})


def charging_figure(readings: list[dict[str, Any]], session_id: str) -> go.Figure:
    """Small multiples sharing time: cumulative energy, power and state of charge."""

    panels = [
        ("energy_kwh", "Energy delivered (kWh)", ".2f"),
        ("power_kw", "Power (kW)", ".1f"),
        ("soc_pct", "State of charge (%)", ".1f"),
    ]
    fig = make_subplots(
        rows=len(panels), cols=1, shared_xaxes=True, vertical_spacing=0.09,
        subplot_titles=[title for _, title, _ in panels],
    )
    times = [item["event_time"] for item in readings]
    for row, (field, title, fmt) in enumerate(panels, start=1):
        fig.add_trace(
            go.Scatter(
                x=times,
                y=[item[field] for item in readings],
                mode="lines+markers" if len(readings) < 3 else "lines",
                line={"color": SERIES, "width": 2},
                marker={"size": 8, "color": SERIES},
                name=title,
                hovertemplate=f"%{{y:{fmt}}}<extra>{title}</extra>",
            ),
            row=row,
            col=1,
        )
    fig.update_annotations(font={"size": 12, "color": INK_SECONDARY}, x=0, xanchor="left")
    _base_layout(fig, 430, session_id)
    fig.update_xaxes(title_text="Event time (simulated)", row=len(panels), col=1)
    return fig


def popup_curve_figure(readings: list[dict[str, Any]], session_id: str) -> go.Figure:
    """Compact live curve for the popup: cumulative energy of one session."""
    fig = go.Figure(
        go.Scatter(
            x=[item["event_time"] for item in readings],
            y=[item["energy_kwh"] for item in readings],
            mode="lines+markers" if len(readings) < 3 else "lines",
            line={"color": SERIES, "width": 2},
            marker={"size": 8, "color": SERIES},
            hovertemplate="%{y:.2f} kWh<extra>Energy delivered</extra>",
        )
    )
    _base_layout(fig, 170, session_id)
    fig.update_layout(margin={"l": 44, "r": 8, "t": 8, "b": 28}, width=330)
    fig.update_yaxes(title_text="kWh", rangemode="tozero")
    fig.update_xaxes(tickformat="%H:%M", nticks=5)
    return fig


def popup_curve(data: DashboardData, station: dict[str, Any]) -> tuple[Any, Any, Any]:
    """Caption, figure and style for the popup's live session curve."""
    hidden = {"display": "none"}
    if station["active_sessions"] == 0:
        return "No active charging session.", go.Figure(), hidden
    active = [s for s in data.station_sessions(station["station_id"]) if s["status"] == "active"]
    if not active:
        return "No active charging session.", go.Figure(), hidden
    session = active[0]
    readings = data.session_readings(session["session_id"])
    more = (
        f" · {len(active) - 1} more active, choose in the side panel" if len(active) > 1 else ""
    )
    vehicle = f" · {session['vehicle_model']}" if session["vehicle_model"] else ""
    caption = (
        f"Live charging session: plug {session['connector_id']} since "
        f"{_clock(session['start_time'])}{vehicle}{more}"
    )
    if not readings:
        return caption + " · waiting for the first meter reading.", go.Figure(), hidden
    latest = readings[-1]
    caption += (
        f" · {latest['energy_kwh']:.2f} kWh, {latest['power_kw']:.0f} kW, "
        f"SoC {latest['soc_pct']:.0f}% at {_clock(latest['event_time'])}"
    )
    return caption, popup_curve_figure(readings, session["session_id"]), {"display": "block", "height": "170px"}


def price_figure(history: list[dict[str, Any]], station_id: str) -> go.Figure:
    fig = go.Figure(
        go.Scatter(
            x=[item["event_time"] for item in history],
            y=[item["price_per_kwh"] for item in history],
            mode="lines+markers",
            line={"color": SERIES, "width": 2, "shape": "hv"},
            marker={"size": 8, "color": SERIES},
            hovertemplate="$%{y:.2f}/kWh<extra>Received price</extra>",
        )
    )
    _base_layout(fig, 200, station_id)
    fig.update_yaxes(title_text="AUD/kWh", tickprefix="$", tickformat=".2f")
    fig.update_xaxes(title_text="Event time (simulated)")
    return fig


def history_table(history: list[dict[str, Any]]) -> Any:
    return html.Table(
        [html.Tr([html.Th("Event time"), html.Th("Price"), html.Th("Received at")])]
        + [
            html.Tr(
                [
                    html.Td(_time(item["event_time"])),
                    html.Td(f"${item['price_per_kwh']:.2f}"),
                    html.Td(_time(item["received_at"])),
                ]
            )
            for item in history
        ],
        className="events",
    )


def readings_table(readings: list[dict[str, Any]]) -> Any:
    return html.Table(
        [html.Tr([html.Th(label) for label in ("Time", "kWh", "kW", "SoC %", "V", "A")])]
        + [
            html.Tr(
                [
                    html.Td(_clock(item["event_time"])),
                    html.Td(f"{item['energy_kwh']:.3f}"),
                    html.Td(f"{item['power_kw']:.1f}"),
                    html.Td(f"{item['soc_pct']:.1f}"),
                    html.Td(f"{item['voltage_v']:.0f}"),
                    html.Td(f"{item['current_a']:.0f}"),
                ]
            )
            for item in readings
        ],
        className="events",
    )


def session_label(session: dict[str, Any]) -> str:
    vehicle = f" · {session['vehicle_model']}" if session["vehicle_model"] else ""
    if session["status"] == "active":
        return f"Active · plug {session['connector_id']} · since {_clock(session['start_time'])}{vehicle}"
    return (
        f"Completed · plug {session['connector_id']} · "
        f"{_clock(session['start_time']) or '?'}–{_clock(session['end_time'])}{vehicle}"
    )


EVENT_LABELS = {
    "price_update": "Price",
    "session_start": "Session start",
    "session_end": "Session end",
}


def recent_events_table(snapshot: dict[str, Any]) -> Any:
    events = snapshot["recent_events"]
    if not events:
        return html.P("No price or session events received yet.", className="muted")
    titles = {station["station_id"]: _title(station) for station in snapshot["stations"]}
    rows = []
    for event in events:
        if event["event_type"] == "price_update":
            detail = f"${event['detail']:.2f}/kWh"
        else:
            detail = f"plug {int(event['detail'])}"
        rows.append(
            html.Tr(
                [
                    html.Td(EVENT_LABELS[event["event_type"]]),
                    html.Td(titles.get(event["station_id"], event["station_id"])),
                    html.Td(_clock(event["event_time"])),
                    html.Td(detail),
                ]
            )
        )
    return html.Table(
        [html.Tr([html.Th("Event"), html.Th("Station"), html.Th("Time"), html.Th("Detail")])] + rows,
        className="events",
    )


def _bounds(snapshot: dict[str, Any]) -> list[list[float]]:
    lats = [station["lat"] for station in snapshot["stations"]]
    lons = [station["lon"] for station in snapshot["stations"]]
    if not lats:
        # Fallback view over NSW when no simulated station has A1 coordinates.
        return [[-37.5, 141.0], [-28.0, 153.7]]
    return [[min(lats), min(lons)], [max(lats), max(lons)]]


def _find(snapshot: dict[str, Any], station_id: str | None) -> dict[str, Any] | None:
    return next(
        (item for item in snapshot["stations"] if item["station_id"] == station_id), None
    )


# ---------------------------------------------------------------- app


def create_app(
    data: DashboardData,
    *,
    refresh_seconds: float = 1.0,
    highlight_seconds: float = 5.0,
    subscriber: Any = None,
) -> Dash:
    initial = data.snapshot()
    app = Dash(__name__, title="EV Charger Monitor")
    app.index_string = app.index_string.replace(
        "{%css%}", "{%css%}<style>" + PAGE_CSS + "</style>"
    )
    graph_config = {"displaylogo": False, "responsive": True}
    app.layout = html.Div(
        [
            html.Div(
                [
                    html.H1("EV charging stations: live prices and charging sessions"),
                    html.Div(id="status", className="status"),
                ],
                className="header",
            ),
            html.Div(
                [
                    html.Div(
                        dl.Map(
                            [
                                dl.TileLayer(),
                                dl.LayerGroup(
                                    build_markers(initial, "price", highlight_seconds),
                                    id="markers",
                                ),
                                dl.LayerGroup(id="popup-layer"),
                            ],
                            bounds=_bounds(initial),
                            # Viewport tracking needs a center prop; without one,
                            # every pan raises in dash-leaflet and breaks popups.
                            trackViewport=False,
                            style={"height": "100%", "width": "100%"},
                        ),
                        className="map-wrap",
                    ),
                    html.Div(
                        [
                            html.Div(
                                [
                                    html.Span("Colour by:", className="muted"),
                                    dcc.RadioItems(
                                        id="colour-mode",
                                        options=[
                                            {"label": " Price", "value": "price"},
                                            {"label": " Occupancy", "value": "occupancy"},
                                        ],
                                        value="price",
                                        inline=True,
                                    ),
                                ],
                                className="controls",
                            ),
                            html.Div(id="legend"),
                            html.H2("Selected station"),
                            html.Div(id="detail"),
                            html.Div(
                                [
                                    html.H2("Charging session"),
                                    dcc.Dropdown(id="session-picker", clearable=False),
                                    html.Div(id="session-note", className="muted"),
                                    dcc.Graph(id="charging-curve", config=graph_config),
                                    html.Details(
                                        [html.Summary("Show readings table"), html.Div(id="readings")]
                                    ),
                                    html.H2("Received price history"),
                                    html.Div(id="price-note", className="muted"),
                                    dcc.Graph(id="price-history", config=graph_config),
                                    html.Details(
                                        [html.Summary("Show price table"), html.Div(id="price-table")]
                                    ),
                                ],
                                id="station-panels",
                                style={"display": "none"},
                            ),
                            html.H2("Recent price and session events"),
                            html.Div(id="recent"),
                        ],
                        className="side",
                    ),
                ],
                className="main",
            ),
            dcc.Interval(id="tick", interval=int(refresh_seconds * 1000)),
            dcc.Store(id="snapshot", data=initial),
            dcc.Store(id="marker-state"),
            dcc.Store(
                id="shown",
                data=[s["station_id"] for s in initial["stations"] if s["has_events"]],
            ),
            dcc.Store(id="selected"),
        ],
        className="app",
    )

    def subscriber_status() -> str:
        if subscriber is None:
            return "Read-only mode (no subscriber in this process)"
        connected = "connected" if subscriber.client.is_connected() else "NOT connected"
        return (
            f"MQTT {connected} · accepted {subscriber.accepted_count} · "
            f"duplicates {subscriber.duplicate_count} · rejected {subscriber.rejected_count} · "
            f"storage errors {subscriber.storage_error_count}"
        )

    @app.callback(
        Output("snapshot", "data"),
        Output("status", "children"),
        Input("tick", "n_intervals"),
    )
    def refresh(_: int | None) -> Any:
        try:
            snapshot = data.snapshot()
        except (duckdb.Error, OSError) as exc:
            # Keep the last good data on screen, e.g. while another process holds the lock.
            error = html.Span(f"Database read failed, showing previous data: {exc}", className="error")
            return dash.no_update, [error, " · ", subscriber_status()]
        stations = snapshot["stations"]
        counts = snapshot["stored_events"]
        priced = sum(station["latest"] is not None for station in stations)
        active = sum(station["active_sessions"] for station in stations)
        clock = _time(snapshot["replay_time"]) or "no events yet"
        on_map = sum(station["has_events"] for station in stations)
        parts = [
            f"{on_map}/{len(stations)} stations on map",
            f"{priced} priced",
            f"{active} active sessions",
            f"stored: {counts['price_update']} prices, {counts['session_start']} starts, "
            f"{counts['session_end']} ends, {counts['meter']} meter readings",
            subscriber_status(),
            f"refreshed {snapshot['queried_at'][11:]}",
        ]
        if snapshot["missing_station_ids"]:
            parts.append(f"{len(snapshot['missing_station_ids'])} simulated stations missing from A1")
        return snapshot, [html.Span(f"Replay time {clock}", className="clock"), " · ", " · ".join(parts)]

    @app.callback(Output("legend", "children"), Input("colour-mode", "value"))
    def show_legend(mode: str) -> Any:
        return legend(mode, highlight_seconds)

    # A station's marker is added when its first message has been stored.
    # Patch appends without resending or re-creating existing markers.
    @app.callback(
        Output("markers", "children"),
        Output("shown", "data"),
        Input("snapshot", "data"),
        State("colour-mode", "value"),
        State("shown", "data"),
    )
    def add_markers(snapshot: dict[str, Any], mode: str, shown: list[str]) -> Any:
        new = [
            station for station in snapshot["stations"]
            if station["has_events"] and station["station_id"] not in shown
        ]
        if not new:
            return dash.no_update, dash.no_update
        patch = Patch()
        for station in new:
            patch.append(build_marker(station, mode, highlight_seconds))
        return patch, shown + [station["station_id"] for station in new]

    # Data updates change marker props in place, so an open popup stays open.
    @app.callback(
        Output({"type": "station", "id": ALL}, "pathOptions"),
        Output({"type": "station", "id": ALL}, "radius"),
        Output({"type": "label", "id": ALL}, "children"),
        Output("marker-state", "data"),
        Input("snapshot", "data"),
        Input("colour-mode", "value"),
        State("marker-state", "data"),
    )
    def update_markers(snapshot: dict[str, Any], mode: str, previous: dict | None) -> Any:
        previous = previous or {}
        by_id = {station["station_id"]: station for station in snapshot["stations"]}
        outputs: list[list[Any]] = [[] for _ in range(3)]
        state = dict(previous)
        for output_id in dash.ctx.outputs_list[0]:
            station = by_id.get(output_id["id"]["id"])
            signature = (
                None if station is None else station_signature(station, mode, highlight_seconds)
            )
            if station is None or previous.get(station["station_id"]) == signature:
                for column in outputs:
                    column.append(dash.no_update)
                continue
            state[station["station_id"]] = signature
            style = marker_style(station, mode, highlight_seconds)
            for column, value in zip(
                outputs,
                (
                    style["pathOptions"],
                    style["radius"],
                    marker_label(station, mode),
                ),
            ):
                column.append(value)
        return (*outputs, state)

    @app.callback(
        Output("selected", "data"),
        Output("popup-layer", "children"),
        Input({"type": "station", "id": ALL}, "n_clicks"),
        State("snapshot", "data"),
        prevent_initial_call=True,
    )
    def select_station(_: list[int | None], snapshot: dict[str, Any]) -> Any:
        # Rebuilt markers also trigger this callback, but with no click count.
        if not dash.ctx.triggered_id or not dash.ctx.triggered[0]["value"]:
            return dash.no_update, dash.no_update
        station_id = dash.ctx.triggered_id["id"]
        station = _find(snapshot, station_id)
        if station is None:
            return station_id, []
        nonce = dash.ctx.triggered[0]["value"]
        return station_id, [station_popup(station, nonce)]

    @app.callback(
        Output({"type": "popup-body", "id": ALL}, "children"),
        Output({"type": "popup-caption", "id": ALL}, "children"),
        Output({"type": "popup-curve", "id": ALL}, "figure"),
        Output({"type": "popup-curve", "id": ALL}, "style"),
        Input("snapshot", "data"),
        State({"type": "popup-body", "id": ALL}, "id"),
    )
    def refresh_popup(snapshot: dict[str, Any], ids: list[dict[str, str]]) -> Any:
        outputs: list[list[Any]] = [[] for _ in range(4)]
        for item in ids:
            station = _find(snapshot, item["id"])
            if station is None:
                values = (dash.no_update,) * 4
            else:
                try:
                    caption, figure, style = popup_curve(data, station)
                except (duckdb.Error, OSError) as exc:
                    caption, figure, style = f"Session data unavailable: {exc}", dash.no_update, dash.no_update
                values = (popup_content(station), caption, figure, style)
            for column, value in zip(outputs, values):
                column.append(value)
        return tuple(outputs)

    @app.callback(
        Output("detail", "children"),
        Output("station-panels", "style"),
        Input("selected", "data"),
        Input("snapshot", "data"),
    )
    def show_detail(station_id: str | None, snapshot: dict[str, Any]) -> Any:
        if station_id is None:
            return html.P("Click a station marker to see its details.", className="muted"), {"display": "none"}
        station = _find(snapshot, station_id)
        if station is None:
            return html.P("The selected station is no longer available.", className="muted"), {"display": "none"}
        return [metric_tiles(station), details_table(station)], {"display": "block"}

    @app.callback(
        Output("session-picker", "options"),
        Output("session-picker", "value"),
        Output("session-note", "children"),
        Input("selected", "data"),
        Input("snapshot", "data"),
        State("session-picker", "value"),
    )
    def update_sessions(station_id: str | None, _: Any, current: str | None) -> Any:
        if station_id is None:
            return [], None, ""
        try:
            sessions = data.station_sessions(station_id)
        except (duckdb.Error, OSError) as exc:
            return dash.no_update, dash.no_update, f"Sessions unavailable: {exc}"
        options = [{"label": session_label(s), "value": s["session_id"]} for s in sessions]
        ids = [s["session_id"] for s in sessions]
        active = sum(s["status"] == "active" for s in sessions)
        if not sessions:
            return [], None, "No sessions received for this station yet."
        value = current if current in ids else ids[0]
        if active == 0:
            note = "No active charging session; showing the most recent completed one."
        elif active == 1:
            note = "One active session."
        else:
            note = f"{active} active sessions; defaults to the most recently started one."
        return options, value, note

    @app.callback(
        Output("charging-curve", "figure"),
        Output("charging-curve", "style"),
        Output("readings", "children"),
        Input("session-picker", "value"),
        Input("snapshot", "data"),
    )
    def update_curve(session_id: str | None, _: Any) -> Any:
        hidden = {"display": "none"}
        if session_id is None:
            return go.Figure(), hidden, ""
        try:
            readings = data.session_readings(session_id)
        except (duckdb.Error, OSError) as exc:
            return dash.no_update, dash.no_update, html.P(f"Readings unavailable: {exc}", className="missing")
        if not readings:
            return go.Figure(), hidden, html.P("Waiting for the first meter reading.", className="muted")
        return charging_figure(readings, session_id), {"display": "block"}, readings_table(readings)

    @app.callback(
        Output("price-history", "figure"),
        Output("price-history", "style"),
        Output("price-table", "children"),
        Output("price-note", "children"),
        Input("selected", "data"),
        Input("snapshot", "data"),
    )
    def update_price_history(station_id: str | None, _: Any) -> Any:
        hidden = {"display": "none"}
        if station_id is None:
            return go.Figure(), hidden, "", ""
        try:
            history = data.price_history(station_id)
        except (duckdb.Error, OSError) as exc:
            return dash.no_update, dash.no_update, "", f"Price history unavailable: {exc}"
        if not history:
            return go.Figure(), hidden, "", "No price history received yet."
        note = f"{len(history)} received price events; the line holds each price until the next."
        return price_figure(history, station_id), {"display": "block"}, history_table(history), note

    @app.callback(Output("recent", "children"), Input("snapshot", "data"))
    def show_recent(snapshot: dict[str, Any]) -> Any:
        return recent_events_table(snapshot)

    return app


def main() -> int:
    args = parse_arguments()
    lock = threading.Lock()
    try:
        stations = load_simulated_stations(args.stations)
        subscriber = start_subscriber(args, lock) if args.subscribe else None
        data = DashboardData(args.database, stations, lock)
        app = create_app(
            data,
            refresh_seconds=args.refresh_seconds,
            highlight_seconds=args.highlight_seconds,
            subscriber=subscriber,
        )
    except (OSError, ValueError, duckdb.Error) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1
    print(f"Dashboard at http://{args.host}:{args.port}/ (database {data.database_path})", flush=True)
    # debug=False: the reloader would start a second process and subscriber.
    app.run(host=args.host, port=args.port, debug=False)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
