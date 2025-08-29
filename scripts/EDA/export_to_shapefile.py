"""
Export a PostGIS table to an ESRI Shapefile using GeoPandas.

Usage examples (PowerShell):

  # Export selected columns from public.admin_table to outputs/admin_table_export.shp
  python scripts/export_to_shapefile.py \
    --table admin_table \
    --schema public \
    --columns id blm_id blm_admin res_name geom \
    --output outputs/admin_table_export.shp

  # Custom connection and WHERE filter
  python scripts/export_to_shapefile.py \
    --host localhost --port 5432 --db lithiumvalley --user postgres --password 1123 \
    --table admin_table --schema public \
    --where "blm_admin IS NOT NULL" \
    --output outputs/admin_table_filtered.shp

Notes:
- Shapefile field names are limited to 10 characters. By default, long column names are
  shortened to remain compatible. Use --no-shorten-field-names to disable.
- Shapefiles cannot store mixed geometry types. Use --explode-collections to split
  multi-part geometries into single-part features when desired.
"""

import os
import sys
import argparse
from pathlib import Path
from typing import List, Optional, Dict

import geopandas as gpd
import pandas as pd
from sqlalchemy import create_engine, text


# Defaults aligned with scripts/postgres_pipe.py
DEFAULT_HOST = "localhost"
DEFAULT_PORT = 5432
DEFAULT_USER = "postgres"
DEFAULT_PASSWORD = "1123"
DEFAULT_DB = "lithiumvalley"


def build_connection_url(host: str, port: int, db: str, user: str, password: str) -> str:
    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"


def detect_geometry_column(engine, schema: str, table: str, fallback: Optional[str] = None) -> str:
    """Try to detect the geometry column via geometry_columns; fall back to a common name."""
    try:
        with engine.begin() as conn:
            df = pd.read_sql(
                text(
                    """
                    SELECT f_geometry_column AS geom_col
                    FROM public.geometry_columns
                    WHERE f_table_schema = :schema AND f_table_name = :table
                    """
                ),
                conn,
                params={"schema": schema, "table": table},
            )
        if not df.empty:
            return str(df.iloc[0]["geom_col"])
    except Exception:
        pass

    # Common geometry column names
    for candidate in [fallback, "geom", "geometry", "wkb_geometry"]:
        if candidate:
            try:
                with engine.begin() as conn:
                    exists_df = pd.read_sql(
                        text(
                            """
                            SELECT column_name
                            FROM information_schema.columns
                            WHERE table_schema = :schema AND table_name = :table AND column_name = :col
                            """
                        ),
                        conn,
                        params={"schema": schema, "table": table, "col": candidate},
                    )
                if not exists_df.empty:
                    return candidate
            except Exception:
                continue

    raise RuntimeError(
        f"Could not detect geometry column for {schema}.{table}. Provide it via --geom-col."
    )


def shorten_field_names(columns: List[str]) -> Dict[str, str]:
    """Return a mapping of long column names -> <=10-char unique names for Shapefile."""
    mapping: Dict[str, str] = {}
    used: Dict[str, int] = {}

    def make_short(name: str) -> str:
        base = name[:10]
        if len(base) <= 10:
            short = base
        else:
            short = base[:10]
        # Ensure uniqueness
        if short not in used:
            used[short] = 0
            return short
        used[short] += 1
        suffix = str(used[short])
        return (short[: 10 - len(suffix)] + suffix)[:10]

    for col in columns:
        if len(col) > 10:
            mapping[col] = make_short(col)
    return mapping


def remove_existing_shapefile(output_path: Path) -> None:
    stem = output_path.with_suffix("")
    for ext in [".shp", ".shx", ".dbf", ".prj", ".cpg"]:
        p = stem.with_suffix(ext)
        if p.exists():
            p.unlink()


def export_table_to_shapefile(
    host: str,
    port: int,
    db: str,
    user: str,
    password: str,
    schema: str,
    table: str,
    output: Path,
    columns: Optional[List[str]] = None,
    geom_col: Optional[str] = None,
    where: Optional[str] = None,
    to_crs: Optional[int] = 4326,
    explode_collections: bool = False,
    shorten_names: bool = True,
    overwrite: bool = True,
):
    engine = create_engine(build_connection_url(host, port, db, user, password))

    # Determine geometry column if not provided
    geom_column = geom_col or detect_geometry_column(engine, schema, table)

    # Build SQL
    if columns:
        if geom_column not in columns:
            columns = list(columns) + [geom_column]
        select_cols = ", ".join([f'"{c}"' for c in columns])
    else:
        select_cols = "*"
    sql = f"SELECT {select_cols} FROM \"{schema}\".\"{table}\""
    if where:
        sql += f" WHERE {where}"

    print(f"Reading from {schema}.{table} (geom_col='{geom_column}')...")
    with engine.begin() as conn:
        gdf = gpd.read_postgis(sql, conn, geom_col=geom_column)

    print(f"Loaded {len(gdf)} rows")
    print(f"Source CRS: {gdf.crs}")

    # Debug: print all unique values in blm_admin if the column exists
    try:
        if "blm_admin" in gdf.columns:
            uniques = pd.unique(gdf["blm_admin"].dropna())
            print(f"blm_admin unique values ({len(uniques)}):")
            for val in uniques:
                print(f"  - {val}")
    except Exception:
        # Non-fatal; continue export
        pass

    # Optionally reproject
    if to_crs is not None:
        if gdf.crs is None or gdf.crs.to_epsg() != to_crs:
            print(f"Reprojecting to EPSG:{to_crs}...")
            gdf = gdf.to_crs(epsg=to_crs)

    # Optionally explode multi-geometries
    if explode_collections:
        try:
            gdf = gdf.explode(index_parts=False, ignore_index=True)
            print("Exploded multi-part geometries into single-part features")
        except TypeError:
            # GeoPandas < 0.10 signature
            gdf = gdf.explode()
            gdf = gdf.reset_index(drop=True)
            print("Exploded multi-part geometries into single-part features")

    # Shapefile field name constraints
    if shorten_names:
        rename_map = shorten_field_names([c for c in gdf.columns if c != geom_column])
        if rename_map:
            print("Shortening long field names for Shapefile:")
            for old, new in rename_map.items():
                print(f"  {old} -> {new}")
            gdf = gdf.rename(columns=rename_map)

    # Ensure output directory exists
    output.parent.mkdir(parents=True, exist_ok=True)
    if overwrite:
        remove_existing_shapefile(output)

    print(f"Writing Shapefile: {output}")
    gdf.to_file(output, driver="ESRI Shapefile", encoding="utf-8")
    print(f"✅ Shapefile exported: {output}")


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export a PostGIS table to ESRI Shapefile")

    # Connection
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("--port", default=DEFAULT_PORT, type=int)
    parser.add_argument("--db", default=DEFAULT_DB)
    parser.add_argument("--user", default=DEFAULT_USER)
    parser.add_argument("--password", default=os.environ.get("PGPASSWORD", DEFAULT_PASSWORD))

    # Table
    parser.add_argument("--schema", default="public")
    parser.add_argument("--table", required=True)
    parser.add_argument("--geom-col", default=None, help="Geometry column name (auto-detect by default)")
    parser.add_argument(
        "--columns", nargs="+", default=None, help="Space-separated list of columns to select"
    )
    parser.add_argument("--where", default=None, help="Optional SQL WHERE clause without the word WHERE")

    # Output
    parser.add_argument(
        "--output",
        default=None,
        help="Output .shp path. Defaults to outputs/<table>.shp",
    )
    parser.add_argument("--to-crs", type=int, default=4326, help="Target EPSG code (default: 4326)")
    parser.add_argument("--no-reproject", action="store_true", help="Do not change CRS")
    parser.add_argument("--explode-collections", action="store_true", help="Explode multi-geometries")
    parser.add_argument("--no-shorten-field-names", action="store_true", help="Do not shorten long field names")
    parser.add_argument("--no-overwrite", action="store_true", help="Do not overwrite existing shapefile")

    args = parser.parse_args(argv)
    if args.output is None:
        args.output = str(Path("outputs") / f"{args.table}.shp")
    return args


def main(argv: Optional[List[str]] = None) -> None:
    args = parse_args(argv)

    to_crs = None if args.no_reproject else args.to_crs
    export_table_to_shapefile(
        host=args.host,
        port=args.port,
        db=args.db,
        user=args.user,
        password=args.password,
        schema=args.schema,
        table=args.table,
        output=Path(args.output),
        columns=args.columns,
        geom_col=args.geom_col,
        where=args.where,
        to_crs=to_crs,
        explode_collections=bool(args.explode_collections),
        shorten_names=not args.no_shorten_field_names,
        overwrite=not args.no_overwrite,
    )


if __name__ == "__main__":
    main()


