#!/usr/bin/env python3
"""Map the sample stations and the latest price received for each (price stage).

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
from dash import ALL, Dash, Input, Output, State, dcc, html

from assignment2.dashboard.queries import DashboardData, load_station_ids


A2_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DATABASE = A2_ROOT / "data/final/project_a2.duckdb"
DEFAULT_STATIONS = A2_ROOT / "data/generated/sample_20/stations.csv"

NO_PRICE_TEXT = "No price yet"
INK = "#0b0b0b"
NO_PRICE_COLOUR = "#898781"
PRICE_COLOUR = "#2a78d6"

PAGE_CSS = """
html, body { height: 100%; }
body { margin: 0; font-family: system-ui, -apple-system, "Segoe UI", sans-serif; color: #0b0b0b; background: #f9f9f7; }
.app { display: flex; flex-direction: column; height: 100vh; }
.header { padding: 10px 16px; border-bottom: 1px solid #e1e0d9; background: #fcfcfb; }
.header h1 { font-size: 18px; margin: 0 0 4px; }
.status { font-size: 13px; color: #52514e; }
.status .clock { color: #0b0b0b; font-weight: 600; }
.status .error { color: #d03b3b; }
.main { display: flex; flex: 1 1 auto; min-height: 0; }
.map-wrap { flex: 1 1 auto; min-width: 0; }
.side { width: 380px; max-width: 45vw; overflow-y: auto; padding: 12px 16px; border-left: 1px solid #e1e0d9; font-size: 13px; background: #fcfcfb; }
.side h2 { font-size: 15px; margin: 16px 0 6px; }
.legend span { display: inline-block; margin-right: 12px; color: #52514e; }
.dot { display: inline-block; width: 10px; height: 10px; border-radius: 50%; margin-right: 4px; vertical-align: middle; }
.ring { display: inline-block; width: 10px; height: 10px; border-radius: 50%; margin-right: 4px; vertical-align: middle; border: 2px solid #0b0b0b; box-sizing: border-box; }
table.details { border-collapse: collapse; width: 100%; }
table.details th { text-align: left; font-weight: 600; padding: 2px 8px 2px 0; vertical-align: top; white-space: nowrap; }
table.details td { padding: 2px 0; word-break: break-word; }
table.events { border-collapse: collapse; width: 100%; font-variant-numeric: tabular-nums; }
table.events th, table.events td { text-align: left; padding: 2px 6px 2px 0; border-bottom: 1px solid #f0efec; }
.missing { color: #a50e0e; font-style: italic; }
.muted { color: #52514e; }
.price { font-size: 18px; font-weight: 600; }
.leaflet-tooltip.value-label { padding: 0 4px; font-size: 11px; font-weight: 600; color: #0b0b0b; background: #fcfcfb; border: 1px solid #c3c2b7; border-radius: 4px; box-shadow: none; font-variant-numeric: tabular-nums; }
.leaflet-tooltip.value-label::before { display: none; }
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
        "--stations", type=Path, default=DEFAULT_STATIONS,
        help="Generator stations.csv defining which stations are shown.",
    )
    parser.add_argument("--refresh-seconds", type=float, default=1.0, help="Database polling interval.")
    parser.add_argument(
        "--highlight-seconds", type=float, default=5.0,
        help="Ring stations whose latest price arrived within this many seconds.",
    )
    parser.add_argument("--host", default="127.0.0.1", help="Dashboard web server host.")
    parser.add_argument("--port", type=int, default=8050, help="Dashboard web server port.")
    parser.add_argument(
        "--subscribe", action="store_true",
        help="Run B's MQTT subscriber in this process and store received prices.",
    )
    parser.add_argument("--mqtt-host", default="localhost", help="MQTT Broker host.")
    parser.add_argument("--mqtt-port", type=int, default=1883, help="MQTT Broker port.")
    parser.add_argument("--mqtt-topic", default="evchargers/+/price", help="MQTT topic filter.")
    parser.add_argument(
        "--mqtt-client-id", default="comp5339-dashboard-subscriber",
        help="MQTT client identifier (differs from the standalone subscriber).",
    )
    parser.add_argument("--username", help="Optional MQTT username.")
    parser.add_argument("--password", help="Optional MQTT password.")
    parser.add_argument(
        "--received-log", type=Path,
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
        host=args.mqtt_host, port=args.mqtt_port, topic=args.mqtt_topic,
        client_id=args.mqtt_client_id, username=args.username, password=args.password,
        output=args.received_log, database=args.database, max_messages=0,
    )
    with lock:
        subscriber = PriceSubscriber(subscriber_args)

    store_event = subscriber.store_event

    def locked_store_event(database_path: Path, event: dict[str, Any]) -> str:
        with lock:
            return store_event(database_path, event)

    subscriber.store_event = locked_store_event
    threading.Thread(target=subscriber.run, name="price-subscriber", daemon=True).start()
    return subscriber


def _time(value: str | None) -> str | None:
    return None if value is None else value.replace("T", " ")


def _clock(value: str | None) -> str:
    return "" if value is None else value[11:19]


def _value(value: Any, unit: str = "") -> Any:
    if value is None:
        return html.Span("Missing", className="missing")
    return f"{value}{unit}"


def _title(station: dict[str, Any]) -> str:
    return station["name"] or station["address"] or station["station_id"]


def _is_recent(station: dict[str, Any], highlight_seconds: float) -> bool:
    latest = station["latest"]
    return latest is not None and latest["age_seconds"] is not None and latest["age_seconds"] <= highlight_seconds


def marker_label(station: dict[str, Any]) -> str:
    latest = station["latest"]
    return f"${latest['price_per_kwh']:.2f}" if latest else NO_PRICE_TEXT


def details_table(station: dict[str, Any]) -> html.Table:
    """A1 details and the latest received price for one station."""

    latest = station["latest"]
    if latest is None:
        price = html.Span(NO_PRICE_TEXT, className="missing")
    else:
        price = html.Span(
            [html.Span(f"${latest['price_per_kwh']:.2f}/kWh", className="price"),
             f" (event time {_clock(latest['event_time'])})"]
        )
    charger_type = f" · A1 charger type: {station['charger_type']}" if station["charger_type"] else ""
    plug_types = (
        f"{station['plug_types']}{charger_type}" if station["plug_types"]
        else html.Span([html.Span("Missing", className="missing"), charger_type])
    )
    rows = [
        ("Name", _value(station["name"])),
        ("Address", _value(station["address"])),
        ("Operator", _value(station["operator"])),
        ("Plug types", plug_types),
        ("Plugs", _value(station["plugs"])),
        ("Power", _value(station["power_kw"], " kW")),
        ("Latest price", price),
    ]
    return html.Table(
        [html.Tr([html.Th(label), html.Td(value)]) for label, value in rows],
        className="details",
    )


def marker_style(station: dict[str, Any], highlight_seconds: float) -> dict[str, Any]:
    recent = _is_recent(station, highlight_seconds)
    # dash-leaflet re-applies colours only when pathOptions changes.
    return {
        "pathOptions": {
            "fillColor": PRICE_COLOUR if station["latest"] else NO_PRICE_COLOUR,
            "fillOpacity": 0.9,
            "color": INK if recent else "#ffffff",
            "weight": 3 if recent else 2,
        },
        "radius": 12 if recent else 8,
    }


def build_markers(snapshot: dict[str, Any], highlight_seconds: float) -> list[Any]:
    markers = []
    for station in snapshot["stations"]:
        station_id = station["station_id"]
        markers.append(
            dl.CircleMarker(
                id={"type": "station", "id": station_id},
                center=[station["lat"], station["lon"]],
                **marker_style(station, highlight_seconds),
                children=[
                    # A permanent label stays open, so its inner text can be updated in place.
                    dl.Tooltip(
                        html.Span(marker_label(station), id={"type": "label", "id": station_id}),
                        permanent=True, direction="right", offset={"x": 8, "y": 0},
                        className="value-label",
                    )
                ],
            )
        )
    return markers


def station_popup(station: dict[str, Any], nonce: Any) -> Any:
    """A fresh popup per click; dash-leaflet loses a reopened popup's content."""

    return dl.Popup(
        html.Div(details_table(station), id={"type": "popup-body", "id": station["station_id"]}),
        id={"type": "popup", "id": station["station_id"], "click": nonce},
        position=[station["lat"], station["lon"]],
        maxWidth=380,
        minWidth=300,
    )


def recent_table(snapshot: dict[str, Any]) -> Any:
    events = snapshot["recent_events"]
    if not events:
        return html.P("No price events received yet.", className="muted")
    titles = {s["station_id"]: _title(s) for s in snapshot["stations"]}
    return html.Table(
        [html.Tr([html.Th("Station"), html.Th("Event time"), html.Th("Price")])]
        + [
            html.Tr([
                html.Td(titles.get(e["station_id"], e["station_id"])),
                html.Td(_clock(e["event_time"])),
                html.Td(f"${e['price_per_kwh']:.2f}"),
            ])
            for e in events
        ],
        className="events",
    )


def _bounds(snapshot: dict[str, Any]) -> list[list[float]]:
    lats = [s["lat"] for s in snapshot["stations"]]
    lons = [s["lon"] for s in snapshot["stations"]]
    if not lats:
        return [[-37.5, 141.0], [-28.0, 153.7]]
    return [[min(lats), min(lons)], [max(lats), max(lons)]]


def _find(snapshot: dict[str, Any], station_id: str | None) -> dict[str, Any] | None:
    return next((s for s in snapshot["stations"] if s["station_id"] == station_id), None)


def create_app(
    data: DashboardData,
    *,
    refresh_seconds: float = 1.0,
    highlight_seconds: float = 5.0,
    subscriber: Any = None,
) -> Dash:
    initial = data.snapshot()
    app = Dash(__name__, title="EV Charger Prices")
    app.index_string = app.index_string.replace("{%css%}", "{%css%}<style>" + PAGE_CSS + "</style>")
    app.layout = html.Div(
        [
            html.Div(
                [html.H1("EV charging stations: latest received prices"), html.Div(id="status", className="status")],
                className="header",
            ),
            html.Div(
                [
                    html.Div(
                        dl.Map(
                            [
                                dl.TileLayer(),
                                dl.LayerGroup(build_markers(initial, highlight_seconds), id="markers"),
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
                                    html.Span([html.I(className="dot", style={"background": NO_PRICE_COLOUR}), NO_PRICE_TEXT]),
                                    html.Span([html.I(className="dot", style={"background": PRICE_COLOUR}), "Has a price"]),
                                    html.Span([html.I(className="ring"), f"New price in last {highlight_seconds:g}s"]),
                                ],
                                className="legend",
                            ),
                            html.H2("Selected station"),
                            html.Div(id="detail"),
                            html.H2("Most recently received prices"),
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

    @app.callback(Output("snapshot", "data"), Output("status", "children"), Input("tick", "n_intervals"))
    def refresh(_: int | None) -> Any:
        try:
            snapshot = data.snapshot()
        except (duckdb.Error, OSError) as exc:
            error = html.Span(f"Database read failed, showing previous data: {exc}", className="error")
            return dash.no_update, [error, " · ", subscriber_status()]
        stations = snapshot["stations"]
        priced = sum(s["latest"] is not None for s in stations)
        parts = [
            f"{priced}/{len(stations)} stations priced",
            f"{snapshot['stored_events']} price events stored",
            subscriber_status(),
            f"refreshed {snapshot['queried_at'][11:]}",
        ]
        if snapshot["missing_station_ids"]:
            parts.append(f"{len(snapshot['missing_station_ids'])} stations missing from A1")
        clock = _time(snapshot["replay_time"]) or "no prices yet"
        return snapshot, [html.Span(f"Latest price time {clock}", className="clock"), " · ", " · ".join(parts)]

    # Data updates change marker props in place, so an open popup stays open.
    @app.callback(
        Output({"type": "station", "id": ALL}, "pathOptions"),
        Output({"type": "station", "id": ALL}, "radius"),
        Output({"type": "label", "id": ALL}, "children"),
        Output("marker-state", "data"),
        Input("snapshot", "data"),
        State("marker-state", "data"),
    )
    def update_markers(snapshot: dict[str, Any], previous: dict | None) -> Any:
        previous = previous or {}
        by_id = {s["station_id"]: s for s in snapshot["stations"]}
        outputs: list[list[Any]] = [[], [], []]
        state = dict(previous)
        for output_id in dash.ctx.outputs_list[0]:
            station = by_id.get(output_id["id"]["id"])
            signature = None if station is None else [
                station["latest"]["event_id"] if station["latest"] else None,
                _is_recent(station, highlight_seconds),
            ]
            if station is None or previous.get(station["station_id"]) == signature:
                for column in outputs:
                    column.append(dash.no_update)
                continue
            state[station["station_id"]] = signature
            style = marker_style(station, highlight_seconds)
            for column, value in zip(outputs, (style["pathOptions"], style["radius"], marker_label(station))):
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
        if not dash.ctx.triggered_id or not dash.ctx.triggered[0]["value"]:
            return dash.no_update, dash.no_update
        station_id = dash.ctx.triggered_id["id"]
        station = _find(snapshot, station_id)
        if station is None:
            return station_id, []
        return station_id, [station_popup(station, dash.ctx.triggered[0]["value"])]

    @app.callback(
        Output({"type": "popup-body", "id": ALL}, "children"),
        Input("snapshot", "data"),
        State({"type": "popup-body", "id": ALL}, "id"),
    )
    def refresh_popup(snapshot: dict[str, Any], ids: list[dict[str, str]]) -> Any:
        bodies = []
        for item in ids:
            station = _find(snapshot, item["id"])
            bodies.append(dash.no_update if station is None else details_table(station))
        return bodies

    @app.callback(Output("detail", "children"), Input("selected", "data"), Input("snapshot", "data"))
    def show_detail(station_id: str | None, snapshot: dict[str, Any]) -> Any:
        if station_id is None:
            return html.P("Click a station marker to see its details.", className="muted")
        station = _find(snapshot, station_id)
        if station is None:
            return html.P("The selected station is no longer available.", className="muted")
        return details_table(station)

    @app.callback(Output("recent", "children"), Input("snapshot", "data"))
    def show_recent(snapshot: dict[str, Any]) -> Any:
        return recent_table(snapshot)

    return app


def main() -> int:
    args = parse_arguments()
    lock = threading.Lock()
    try:
        station_ids = load_station_ids(args.stations)
        subscriber = start_subscriber(args, lock) if args.subscribe else None
        data = DashboardData(args.database, station_ids, lock)
        app = create_app(
            data, refresh_seconds=args.refresh_seconds,
            highlight_seconds=args.highlight_seconds, subscriber=subscriber,
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
