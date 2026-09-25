from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
import platform
import tempfile
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import geopandas as gpd
import pandas as pd


NEW_FIELDS = (
    'connector_types', 'external_connection_quantity', 'external_number_of_points',
    'external_status', 'external_usage_type', 'external_usage_cost',
)


def read_csv(path):
    with path.open(encoding='utf-8-sig', newline='') as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames or len(reader.fieldnames) != len(set(reader.fieldnames)):
            raise ValueError(f'Invalid header: {path.name}')
        rows = list(reader)
        if any(None in row or any(v is None for v in row.values()) for row in rows):
            raise ValueError(f'Malformed CSV row: {path.name}')
        return reader.fieldnames, rows


def index_rows(rows, name):
    result = {}
    for row in rows:
        key = row['record_id']
        if not key or key in result:
            raise ValueError(f'{name}: blank or duplicate record_id: {key}')
        result[key] = row
    return result


def location_key(row):
    # Identical to C: Python round, six decimal places, not a physical-site ID.
    return (round(float(row['Latitude']), 6), round(float(row['Longitude']), 6))


def load_spatial(con, root):
    cache = root / '.duckdb_extensions'
    cache.mkdir(exist_ok=True)
    con.execute("SET extension_directory = '" + str(cache).replace("'", "''") + "'")
    try:
        con.execute('LOAD spatial')
    except duckdb.Error:
        con.execute('INSTALL spatial')
        con.execute('LOAD spatial')


def connect_database(root, read_only=True):
    root = Path(root).resolve()
    con = duckdb.connect(str(root / 'data/final/project.duckdb'), read_only=read_only)
    load_spatial(con, root)
    return con


def insert_rows(con, table, columns, rows):
    if not rows:
        return
    names = ', '.join('"' + n.replace('"', '""') + '"' for n in columns)
    slots = ', '.join('?' for _ in columns)
    con.executemany(
        f'INSERT INTO {table} ({names}) VALUES ({slots})',
        [[None if row[c] == '' else row[c] for c in columns] for row in rows],
    )


def build_database(root):
    root = Path(root).resolve()
    processed = root / 'data/processed'
    files = [processed / n for n in (
        'chargers_clean.csv', 'charger_sa4.csv',
        'charger_attributes.csv', 'charger_sa4_review.csv',
    )]
    boundary_zip = root / 'data/raw/SA4_2026_AUST_SHP_GDA2020.zip'
    for path in [*files, boundary_zip, root / 'sql/schema.sql']:
        if not path.is_file():
            raise FileNotFoundError(path)
    (a_cols, a), (b_cols, b), (c_cols, c), (review_cols, review) = map(read_csv, files)
    if not a:
        raise ValueError('The base dataset is empty')
    aa, bb = index_rows(a, 'A'), index_rows(b, 'B')
    if aa.keys() != bb.keys():
        raise ValueError('A/B record_id sets differ')
    # B carries a copy of A. Reject stale handoffs, but tolerate numeric formatting.
    numeric = {'Latitude', 'Longitude', 'Number_of_plugs', 'power_kw'}
    for key, row in aa.items():
        for col in a_cols:
            left, right = row[col], bb[key][col]
            same = left == right
            if not same and col in numeric and left and right:
                same = float(left) == float(right)
            if not same:
                raise ValueError(f'B has stale or changed A data: {key}, {col}')
        lat, lon = float(row['Latitude']), float(row['Longitude'])
        if not (math.isfinite(lat) and math.isfinite(lon) and -90 <= lat <= 90 and -180 <= lon <= 180):
            raise ValueError(f'Invalid coordinates: {key}')
    dc = {k: row for k, row in aa.items() if row['charger_type_standardized'] == 'DC'}
    if not dc:
        raise ValueError('No DC records')
    c_keys = set()
    for row in c:
        key = (row['record_id'], row['external_source'], row['external_station_id'])
        if not all(key) or key in c_keys or row['record_id'] not in dc:
            raise ValueError(f'Invalid, duplicate or non-DC augmentation key: {key}')
        c_keys.add(key)
        if row['match_decision'] not in {'manual_accept', 'rule_based_accept'}:
            raise ValueError(f'Unaccepted C match: {key}')
        if not any(row[f].strip() for f in NEW_FIELDS):
            raise ValueError(f'C match has no new attribute: {key}')
    for row in b:
        expected_status = 'matched_within' if row['SA4_CODE26'] else 'unmatched'
        if row['sa4_match_status'] != expected_status:
            raise ValueError('B match status conflicts with SA4 code')
        if row['charger_crs_assumed'] != 'EPSG:4326' or row['sa4_boundary_crs'] != 'EPSG:7844':
            raise ValueError('Unexpected CRS: review the geometry conversion')
    for row in review:
        if row['record_id'] not in aa or bb[row['record_id']]['SA4_CODE26']:
            raise ValueError('Review candidate must refer to an unmatched base record')

    final = root / 'data/final'
    final.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(prefix='building-', suffix='.duckdb', dir=final)
    os.close(fd)
    Path(temp_name).unlink()  # DuckDB needs a new path, not an empty file.
    con = None
    try:
        con = duckdb.connect(temp_name)
        load_spatial(con, root)
        con.execute((root / 'sql/schema.sql').read_text(encoding='utf-8'))
        con.execute('BEGIN TRANSACTION')
        operators = sorted({r['operator_standardized'] for r in a if r['operator_standardized']})
        con.executemany('INSERT INTO operators VALUES (?)', [(x,) for x in operators])
        insert_rows(con, 'charger_records', a_cols, a)
        con.execute('UPDATE charger_records SET geom = ST_Point(Longitude, Latitude)')

        with tempfile.TemporaryDirectory(prefix='sa4-') as temp_dir:
            target = Path(temp_dir)
            with zipfile.ZipFile(boundary_zip) as archive:
                for member in archive.infolist():
                    if not (target / member.filename).resolve().is_relative_to(target.resolve()):
                        raise ValueError('Unsafe boundary archive path')
                archive.extractall(target)
            shapes = list(target.rglob('*.shp'))
            if len(shapes) != 1:
                raise ValueError('Expected one SA4 shapefile')
            for ext in ('.shx', '.dbf', '.prj'):
                if not shapes[0].with_suffix(ext).exists():
                    raise FileNotFoundError(f'Missing shapefile component: {ext}')
            regions = gpd.read_file(shapes[0])
            if regions.crs is None or regions.crs.to_epsg() != 7844:
                raise ValueError(f'Unexpected boundary CRS: {regions.crs}')
            region_ids = set(regions['SA4_CODE26'].astype(str))
            if len(region_ids) != len(regions):
                raise ValueError('Duplicate SA4 codes')
            region_lookup = regions.set_index('SA4_CODE26')
            for row in b:
                code = row['SA4_CODE26']
                if code and (code not in region_ids or any(
                    str(region_lookup.loc[code, col]) != row[col]
                    for col in ('SA4_NAME26', 'STE_NAME26')
                )):
                    raise ValueError('B region attributes differ from the supplied boundary')
            for _, row in regions.iterrows():
                geom = row.geometry
                con.execute(
                    'INSERT INTO sa4_regions VALUES (?, ?, ?, ?, ?, ?, ?, ST_GeomFromWKB(?))',
                    [str(row.SA4_CODE26), str(row.SA4_NAME26), str(row.STE_CODE26),
                     str(row.STE_NAME26), float(row.AREASQKM26), 'EPSG:7844',
                     'ASGS Edition 4, SA4 2026', None if pd.isna(geom) else geom.wkb],
                )
        b_extra = ['record_id'] + [col for col in b_cols if col not in a_cols]
        insert_rows(con, 'charger_sa4', b_extra, b)
        insert_rows(con, 'spatial_review', review_cols, review)
        insert_rows(con, 'charger_attributes', c_cols, c)
        connectors = []
        for row in c:
            for connector in sorted({v.strip() for v in row['connector_types'].split('|') if v.strip()}):
                connectors.append([row['record_id'], row['external_source'], row['external_station_id'], connector])
        if connectors:
            con.executemany('INSERT INTO attribute_connectors VALUES (?, ?, ?, ?)', connectors)

        # Full independent SQL point-in-polygon recheck in B's boundary CRS.
        con.execute('''CREATE TEMP TABLE spatial_recheck AS
            SELECT c.record_id, r.SA4_CODE26
            FROM charger_records c LEFT JOIN sa4_regions r
            ON r.geom IS NOT NULL AND ST_Within(
                ST_Transform(c.geom, 'EPSG:4326', 'EPSG:7844', always_xy := true), r.geom)''')
        mismatch = con.execute('''SELECT count(*) FROM spatial_recheck x
            JOIN charger_sa4 b USING (record_id)
            WHERE x.SA4_CODE26 IS DISTINCT FROM b.SA4_CODE26''').fetchone()[0]
        if mismatch or con.execute('SELECT count(*) FROM spatial_recheck').fetchone()[0] != len(a):
            raise ValueError(f'Spatial recheck differs from B: {mismatch} differing rows')
        invalid_regions = con.execute('SELECT count(*) FROM sa4_regions WHERE geom IS NOT NULL AND NOT ST_IsValid(geom)').fetchone()[0]
        if invalid_regions:
            raise ValueError(f'Invalid region geometries: {invalid_regions}')

        accepted_ids = {r['record_id'] for r in c}
        locations = {location_key(row) for row in dc.values()}
        augmented_locations = {location_key(dc[key]) for key in accepted_ids}
        caches = ['ocm_pilot_raw.json', 'ocm_australia_raw.json',
                  'ocm_pilot_candidate_review.csv', 'ocm_full_candidate_review.csv']
        missing_caches = [name for name in caches if not (root / 'data/external' / name).is_file()]
        report = {
            'built_at_utc': datetime.now(timezone.utc).isoformat(),
            'python_version': platform.python_version(), 'duckdb_version': duckdb.__version__,
            'geopandas_version': gpd.__version__,
            'base_records': len(a), 'operators': len(operators),
            'sa4_regions': len(regions),
            'sa4_without_geometry': con.execute('SELECT count(*) FROM sa4_regions WHERE geom IS NULL OR ST_IsEmpty(geom)').fetchone()[0],
            'spatial_matched': sum(bool(row['SA4_CODE26']) for row in b),
            'spatial_unmatched': sum(not row['SA4_CODE26'] for row in b),
            'spatial_review_rows': len(review), 'spatial_recheck_mismatches': mismatch,
            'invalid_region_geometries': invalid_regions,
            'dc_records': len(dc), 'dc_locations_6dp': len(locations),
            'augmentation_rows': len(c), 'augmented_dc_records': len(accepted_ids),
            'augmented_dc_locations_6dp': len(augmented_locations),
            'dc_record_coverage': len(accepted_ids) / len(dc),
            'dc_location_coverage': len(augmented_locations) / len(locations),
            'coverage_target_met': len(augmented_locations) * 2 >= len(locations),
            'match_distance_over_3km': sum(float(r['match_distance_m']) > 3000 for r in c),
            'missing_c_external_files': missing_caches,
            'limitations': [
                'record_id identifies a source record, not a unique physical site.',
                'Location coverage uses Python round(latitude/longitude, 6), matching C.',
                'Charger CRS remains an unconfirmed WGS84 assumption inherited from B.',
                'Nearest SA4 candidates are unresolved and are not assigned to chargers.',
                'External last verification is not the retrieval time; retrieval time is unavailable without the caches.',
                'Long-distance matches and reused external IDs need upstream review; no automatic reassignment.',
            ],
        }
        for table, expected in [('charger_records', len(a)), ('charger_sa4', len(b)),
                                ('charger_attributes', len(c)), ('spatial_review', len(review))]:
            actual = con.execute(f'SELECT count(*) FROM {table}').fetchone()[0]
            if actual != expected:
                raise ValueError(f'Row count changed in {table}')
        report['input_sha256'] = {}
        tracked = [*files, boundary_zip, root / 'sql/schema.sql', Path(__file__).resolve()]
        for path in tracked:
            relative = path.relative_to(root).as_posix()
            digest = hashlib.sha256(path.read_bytes()).hexdigest()
            report['input_sha256'][relative] = digest
            con.execute('INSERT INTO build_inputs VALUES (?, ?, ?, ?)',
                        [relative, digest, path.stat().st_size, report['built_at_utc']])
        con.execute('INSERT INTO build_metadata VALUES (?, ?)', ['quality_report', json.dumps(report)])
        con.execute('COMMIT')
        con.execute('CHECKPOINT')
        con.close()
        con = None
        # Only publish a fully validated, closed database; previous successful build survives failures.
        Path(temp_name).replace(final / 'project.duckdb')
        (final / 'quality_report.json').write_text(json.dumps(report, indent=2, ensure_ascii=False) + '\n', encoding='utf-8')
        with connect_database(root) as check:
            for name, sql in {
                'augmentation_distance_review.csv': 'SELECT * FROM augmentation_review',
                'sa4_summary.csv': 'SELECT * FROM sa4_summary ORDER BY SA4_CODE26',
            }.items():
                cursor = check.execute(sql)
                with (final / name).open('w', encoding='utf-8', newline='') as handle:
                    writer = csv.writer(handle)
                    writer.writerow([c[0] for c in cursor.description])
                    writer.writerows(cursor.fetchall())
        return report
    finally:
        if con is not None:
            con.close()
        for path in (Path(temp_name), Path(temp_name + '.wal')):
            if path.exists():
                path.unlink()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path(__file__).resolve().parents[1])
    args = parser.parse_args()
    print(json.dumps(build_database(args.root), indent=2, ensure_ascii=False))
