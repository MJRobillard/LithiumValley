"""
Generalized spatial data preprocessing pipeline.

This library processes multiple spatial data formats with configurable parameters:
- Shapefiles (.shp) → PostGIS import or GeoDataFrame return
- KMZ files (.kmz) → GeoDataFrame conversion with layer selection
- CSV files (.csv) → Point geometry creation with coordinate parsing

All outputs are normalized to a target CRS and can be filtered by bounding box.

Usage:
    from spatial_data_pipe import SpatialDataProcessor
    
    processor = SpatialDataProcessor(
        target_crs=4326,
        bbox_filter=(-180, -90, 180, 90),  # Optional
        output_dir="outputs"
    )
    
    # Process files
    gdf = processor.process_file("data.shp")
    
    # Batch process
    processor.process_batch(["file1.shp", "file2.kmz", "file3.csv"])
    
    # PostGIS import
    processor.import_to_postgis("file.shp", db_config)

Dependencies:
    pip install pandas geopandas shapely fiona pyproj pyogrio psycopg2-binary sqlalchemy
"""

from __future__ import annotations

import os
import sys
import math
import zipfile
import re
from pathlib import Path
from typing import Iterable, Optional, Tuple, Union, Dict, Any
import argparse
from dataclasses import dataclass

import pandas as pd
import xml.etree.ElementTree as ET
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse

# Spatial data handling
import geopandas as gpd
from shapely.geometry import Point
import shapely.wkb as wkb
from shapely.geometry import box

# Database connectivity
from sqlalchemy import create_engine

# Optional fast IO backend
try:
    import pyogrio
    HAS_PYOGRIO = True
except ImportError:
    HAS_PYOGRIO = False

try:
    import fiona
    HAS_FIONA = True
except ImportError:
    HAS_FIONA = False

try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False


@dataclass
class DatabaseConfig:
    """Database connection configuration."""
    host: str = "localhost"
    port: int = 5432
    user: str = "postgres"
    password: str = ""
    database: str = "spatial_db"
    schema: str = "public"
    geom_column: str = "geom"


@dataclass
class ProcessingConfig:
    """Processing configuration parameters."""
    target_crs: int = 4326
    bbox_filter: Optional[Tuple[float, float, float, float]] = None  # (min_lon, min_lat, max_lon, max_lat)
    state_filter: Optional[str] = None
    county_filter: Optional[Iterable[str]] = None
    preferred_layers: Optional[Iterable[str]] = None
    verbose: bool = False
    drop_z_dimension: bool = True


class SpatialDataProcessor:
    """Generalized spatial data processor for multiple file formats."""
    
    def __init__(
        self,
        config: ProcessingConfig,
        output_dir: Optional[Union[str, Path]] = None,
        db_config: Optional[DatabaseConfig] = None
    ):
        """
        Initialize the spatial data processor.
        
        Args:
            config: Processing configuration
            output_dir: Output directory for processed files
            db_config: Database configuration for PostGIS imports
        """
        self.config = config
        self.output_dir = Path(output_dir) if output_dir else Path("outputs")
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.db_config = db_config
        
        # Validate CRS
        if not isinstance(self.config.target_crs, int):
            raise ValueError("target_crs must be an integer EPSG code")
    
    def ensure_target_crs(self, gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """Ensure GeoDataFrame is in the target CRS."""
        if gdf.empty:
            return gdf.set_crs(self.config.target_crs, allow_override=True) if gdf.crs is None else gdf.to_crs(self.config.target_crs)
        
        if gdf.crs is None:
            # Assume source lon/lat WGS84 when missing
            gdf = gdf.set_crs(4326)
        
        if gdf.crs.to_epsg() != self.config.target_crs:
            gdf = gdf.to_crs(self.config.target_crs)
        
        return gdf
    
    def filter_bbox(self, gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """Filter GeoDataFrame to bounding box if specified."""
        if self.config.bbox_filter is None:
            return gdf
        
        (min_lon, min_lat, max_lon, max_lat) = self.config.bbox_filter
        return gdf.cx[min_lon:max_lon, min_lat:max_lat]
    
    def sanitize_name(self, name: str) -> str:
        """Sanitize name for file/table naming."""
        name = name.strip()
        name = re.sub(r"\s+", "_", name)
        name = re.sub(r"[^A-Za-z0-9_]+", "", name)
        return name or "layer"
    
    def sanitize_table_name(self, name: str) -> str:
        """Sanitize name for PostgreSQL table naming."""
        s = name.lower()
        s = re.sub(r"[^a-z0-9]+", "_", s)
        s = re.sub(r"_+", "_", s).strip("_")
        if not s:
            s = "layer"
        if s[0].isdigit():
            s = f"t_{s}"
        # Postgres identifier max length is 63
        return s[:63]
    
    def write_outputs(
        self,
        gdf: gpd.GeoDataFrame,
        basename: str,
        layer_name: Optional[str] = None,
    ) -> None:
        """Write a GeoDataFrame to Shapefile and append to a GeoPackage."""
        gdf = self.ensure_target_crs(gdf)

        shp_path = self.output_dir / f"{basename}.shp"
        gpkg_path = self.output_dir / "spatial_layers.gpkg"
        _layer = layer_name or basename

        # Prefer pyogrio for reliability on Windows; fallback to GeoPandas/Fiona
        if HAS_PYOGRIO:
            from pyogrio import write_dataframe
            write_dataframe(gdf, shp_path, driver="ESRI Shapefile")
            write_dataframe(gdf, gpkg_path, driver="GPKG", layer=_layer)
        else:
            gdf.to_file(shp_path, driver="ESRI Shapefile")
            gdf.to_file(gpkg_path, layer=_layer, driver="GPKG")
    
    def process_shapefile(
        self,
        shp_path: Path,
        return_gdf: bool = True,
        postgis_import: bool = False,
        table_name: Optional[str] = None
    ) -> Optional[gpd.GeoDataFrame]:
        """Process a shapefile - reproject, filter, and optionally import to PostGIS."""
        if not shp_path.exists():
            if self.config.verbose:
                print(f"Shapefile not found: {shp_path}")
            return None

        if self.config.verbose:
            print(f"Reading shapefile: {shp_path}")
        
        gdf = gpd.read_file(shp_path)
        if self.config.verbose:
            print(f"Loaded {len(gdf)} features")
            print(f"Detected CRS: {gdf.crs}")

        if gdf.crs is None:
            # Attempt to read .prj file next to the .shp
            prj_path = shp_path.with_suffix('.prj')
            if prj_path.exists():
                try:
                    from pyproj import CRS
                    prj_text = prj_path.read_text(encoding='utf-8', errors='ignore')
                    inferred_crs = CRS.from_wkt(prj_text)
                    gdf = gdf.set_crs(inferred_crs, allow_override=True)
                    if self.config.verbose:
                        print(f"CRS inferred from .prj: {inferred_crs.to_string()}")
                except Exception as exc:
                    if self.config.verbose:
                        print(f"Warning: Failed to parse .prj file at {prj_path}: {exc}")
            if gdf.crs is None:
                if self.config.verbose:
                    print(
                        "Error: Shapefile has no CRS and .prj could not be parsed. "
                        "Please define a source CRS before import (e.g., re-save with EPSG:4326)."
                    )
                return None

        # Reproject to target CRS
        gdf = self.ensure_target_crs(gdf)
        
        # Apply bbox filter if specified
        if self.config.bbox_filter is not None:
            gdf = self.filter_bbox(gdf)
            if self.config.verbose:
                print(f"After bbox filtering: {len(gdf)} features")

        # Drop Z/M to standardize to 2D if requested
        if self.config.drop_z_dimension:
            def drop_z(geom):
                if geom is None:
                    return geom
                try:
                    return wkb.loads(wkb.dumps(geom, output_dimension=2))
                except Exception:
                    return geom
            gdf["geometry"] = gdf.geometry.apply(drop_z)

        # Ensure geometry column is named as expected
        try:
            gdf = gdf.rename_geometry(self.db_config.geom_column if self.db_config else "geometry")
        except Exception:
            # Fallback for older GeoPandas: rename column and set geometry explicitly
            current_geom_col = gdf.geometry.name
            target_col = self.db_config.geom_column if self.db_config else "geometry"
            if current_geom_col != target_col:
                gdf = gdf.rename(columns={current_geom_col: target_col})
                gdf = gdf.set_geometry(target_col)

        if return_gdf:
            if self.config.verbose:
                print(f"Returning gdf with {len(gdf)} features")
            return gdf

        # Only attempt PostGIS import if both postgis_import is True AND db_config exists
        if postgis_import and self.db_config:
            self._import_to_postgis(gdf, table_name or self.sanitize_table_name(shp_path.stem))
        elif postgis_import and not self.db_config:
            if self.config.verbose:
                print("Warning: PostGIS import requested but no database configuration provided. Skipping import.")
        
        return None
    
    def _import_to_postgis(self, gdf: gpd.GeoDataFrame, table_name: str) -> None:
        """Import GeoDataFrame to PostGIS database."""
        if not self.db_config:
            raise ValueError("Database configuration required for PostGIS import")
        
        engine = create_engine(
            f"postgresql+psycopg2://{self.db_config.user}:{self.db_config.password}@{self.db_config.host}:{self.db_config.port}/{self.db_config.database}"
        )

        if self.config.verbose:
            print("Connecting to database and ensuring PostGIS is enabled...")
        
        with engine.begin() as conn:
            conn.exec_driver_sql("CREATE EXTENSION IF NOT EXISTS postgis;")

            # Drop table if it exists to ensure fresh import
            if self.config.verbose:
                print(f"Dropping table {self.db_config.schema}.{table_name} if it exists...")
            conn.exec_driver_sql(f"DROP TABLE IF EXISTS {self.db_config.schema}.{table_name};")

            if self.config.verbose:
                print(f"Writing GeoDataFrame to {self.db_config.schema}.{table_name}...")
            
            gdf.to_postgis(
                name=table_name,
                con=conn,
                schema=self.db_config.schema,
                if_exists="replace",
                index=False,
            )

            if self.config.verbose:
                print("Creating spatial index and analyzing table...")
            
            conn.exec_driver_sql(
                f"CREATE INDEX IF NOT EXISTS {table_name}_geom_gix ON {self.db_config.schema}.{table_name} USING GIST ({self.db_config.geom_column});"
            )
            
            # Ensure SRID is set to target CRS if writer did not assign it
            conn.exec_driver_sql(
                f"UPDATE {self.db_config.schema}.{table_name} SET {self.db_config.geom_column} = ST_SetSRID({self.db_config.geom_column}, {self.config.target_crs}) WHERE ST_SRID({self.db_config.geom_column}) = 0;"
            )
            conn.exec_driver_sql(f"ANALYZE {self.db_config.schema}.{table_name};")

        if self.config.verbose:
            print("PostGIS import completed successfully.")
    
    def parse_dms(self, dms: str) -> Optional[float]:
        """Parse DMS strings like '56-49.95 N' or '135-22.25 W' to decimal degrees."""
        if pd.isna(dms):
            return None
        s = str(dms).strip()
        if not s:
            return None

        # Replace unicode variants and normalize separators
        s = s.replace("\u00b0", "-").replace("°", "-").replace("--", "-")
        parts = s.split()
        if len(parts) == 2:
            angle, hemi = parts
            hemi = hemi.upper()
        else:
            # No hemisphere? Try to infer later; fail fast
            angle, hemi = s, ""

        # Accept formats: DD-MM.M, DD-MM-SS.S, or decimal
        nums = angle.split("-")
        try:
            if len(nums) == 1 and hemi in {"N", "S", "E", "W"}:
                deg = float(nums[0])
            elif len(nums) == 2:
                deg_i = float(nums[0])
                min_or_sec = float(nums[1])
                # Interpret second part as minutes (common in data seen)
                deg = deg_i + (min_or_sec / 60.0)
            elif len(nums) >= 3:
                deg_i = float(nums[0])
                minutes = float(nums[1])
                seconds = float(nums[2])
                deg = deg_i + (minutes / 60.0) + (seconds / 3600.0)
            else:
                return None
        except ValueError:
            # Try replace unusual characters and retry once
            try:
                angle = angle.replace("N", "").replace("S", "").replace("E", "").replace("W", "")
                nums = angle.split("-")
                if len(nums) == 1:
                    deg = float(nums[0])
                elif len(nums) == 2:
                    deg = float(nums[0]) + float(nums[1]) / 60.0
                elif len(nums) >= 3:
                    deg = float(nums[0]) + float(nums[1]) / 60.0 + float(nums[2]) / 3600.0
                else:
                    return None
            except Exception:
                return None

        if hemi in {"S", "W"}:
            deg = -abs(deg)
        elif hemi in {"N", "E", ""}:
            deg = abs(deg)
        else:
            # Unknown hemisphere token
            return None

        # Clamp to valid ranges
        if not (-180 <= deg <= 180):
            return None
        return deg
    
    def process_csv_to_points(
        self,
        csv_path: Path,
        lat_col: Optional[str] = None,
        lon_col: Optional[str] = None,
        postgis_import: bool = False
    ) -> gpd.GeoDataFrame:
        """Process CSV file to create point geometries with coordinate parsing."""
        if not csv_path.exists():
            raise FileNotFoundError(csv_path)

        if self.config.verbose:
            print(f"Reading CSV: {csv_path}")
        
        df = pd.read_csv(csv_path)

        # Auto-detect coordinate columns if not specified
        if lat_col is None or lon_col is None:
            lat_col_candidates = ["LATITUDE", "Lat", "lat", "LAT", "Y", "y"]
            lon_col_candidates = ["LONGITUDE", "Lon", "lon", "LON", "X", "x"]

            def pick(colnames):
                for c in colnames:
                    if c in df.columns:
                        return c
                return None

            lat_col = lat_col or pick(lat_col_candidates)
            lon_col = lon_col or pick(lon_col_candidates)

        if lat_col is None or lon_col is None:
            raise ValueError(f"Coordinate columns not found. Available columns: {list(df.columns)}")

        if self.config.verbose:
            print(f"Using columns: {lat_col}, {lon_col}")

        # Parse coordinates (support DMS-like strings)
        lat_dd = df[lat_col].apply(self.parse_dms)
        lon_dd = df[lon_col].apply(self.parse_dms)

        # Drop rows with invalid coords
        valid_mask = lat_dd.notna() & lon_dd.notna()
        df_valid = df.loc[valid_mask].copy()
        lat_dd = lat_dd.loc[valid_mask]
        lon_dd = lon_dd.loc[valid_mask]

        if self.config.verbose:
            print(f"Valid coordinates: {len(df_valid)} out of {len(df)} rows")

        # Apply optional filters
        if self.config.state_filter and "STATE" in df_valid.columns:
            df_valid = df_valid[df_valid["STATE"].astype(str).str.upper() == self.config.state_filter.upper()]
        if self.config.county_filter and "COUNTY" in df_valid.columns:
            df_valid = df_valid[df_valid["COUNTY"].astype(str).str.upper().isin({c.upper() for c in self.config.county_filter})]

        # Re-align lat/lon series to the filtered DataFrame's index
        lat_dd = lat_dd.reindex(df_valid.index)
        lon_dd = lon_dd.reindex(df_valid.index)

        geometry = [Point(xy) for xy in zip(lon_dd.tolist(), lat_dd.tolist())]
        gdf = gpd.GeoDataFrame(df_valid.reset_index(drop=True), geometry=geometry, crs=4326)

        # Apply bbox filter if specified
        if self.config.bbox_filter is not None:
            gdf = self.filter_bbox(gdf)
            if self.config.verbose:
                print(f"After bbox filtering: {len(gdf)} features")

        # Normalize CRS
        gdf = self.ensure_target_crs(gdf)

        return gdf
    
    def _parse_kml_network_links(self, kml_path: Path) -> list[str]:
        """Extract NetworkLink href URLs from a KML file."""
        try:
            tree = ET.parse(kml_path)
            root = tree.getroot()
            ns = {"kml": "http://www.opengis.net/kml/2.2"}
            hrefs = []
            for href in root.findall(".//kml:NetworkLink/kml:Link/kml:href", ns):
                if href.text:
                    hrefs.append(href.text.strip())
            return hrefs
        except Exception:
            return []

    def _tweak_kmlserver_url(self, url: str) -> str:
        """Attempt to coerce ArcGIS KmlServer URL to return vectors when possible."""
        try:
            parsed = urlparse(url)
            qs = dict(parse_qsl(parsed.query))
            # Prefer vectors, avoid rasterization
            qs["VectorsToRasters"] = "false"
            # Some servers honor 'f=kmz' or 'format=kmz'
            qs.setdefault("f", "kmz")
            new_query = urlencode(qs, doseq=True)
            return urlunparse(parsed._replace(query=new_query))
        except Exception:
            return url

    def _download_linked_kmls(self, hrefs: list[str], temp_dir: Path) -> list[Path]:
        """Download linked KML/KMZ files from NetworkLinks."""
        downloaded: list[Path] = []
        if not HAS_REQUESTS or not hrefs:
            return downloaded
        
        temp_dir.mkdir(parents=True, exist_ok=True)
        headers = {"User-Agent": "SpatialDataProcessor/1.0"}
        
        for href in hrefs:
            try:
                url = self._tweak_kmlserver_url(href)
                resp = requests.get(url, headers=headers, timeout=20)
                resp.raise_for_status()
                ctype = resp.headers.get("Content-Type", "").lower()
                
                if ".kmz" in url.lower() or "kmz" in ctype:
                    out_path = temp_dir / (self.sanitize_name(Path(urlparse(url).path).stem) + ".kmz")
                    out_path.write_bytes(resp.content)
                    # Unzip KMZ to KMLs
                    with zipfile.ZipFile(out_path) as zf:
                        for name in zf.namelist():
                            if name.lower().endswith(".kml"):
                                zf.extract(name, temp_dir)
                                downloaded.append(temp_dir / name)
                else:
                    # Assume KML
                    out_path = temp_dir / (self.sanitize_name(Path(urlparse(url).path).stem) + ".kml")
                    out_path.write_bytes(resp.content)
                    downloaded.append(out_path)
            except Exception:
                continue
        return downloaded

    def _fetch_arcgis_layers_from_kmlserver(self, hrefs: list[str], temp_dir: Path) -> list[gpd.GeoDataFrame]:
        """Fallback: Use ArcGIS MapServer Feature Query to fetch vectors when KML only has NetworkLinks."""
        gdfs: list[gpd.GeoDataFrame] = []
        if not HAS_REQUESTS:
            return gdfs

        # Determine envelope from bbox filter or use global bounds
        bbox = None
        if self.config.bbox_filter is not None:
            (min_lon, min_lat, max_lon, max_lat) = self.config.bbox_filter
            bbox = (min_lon, min_lat, max_lon, max_lat)
        else:
            bbox = (-180, -90, 180, 90)  # Global bounds

        headers = {"User-Agent": "SpatialDataProcessor/1.0"}

        for href in hrefs:
            try:
                parsed = urlparse(href)
                if not parsed.path.lower().endswith("/kmlserver"):
                    continue
                # Base MapServer endpoint
                base_mapserver = urlunparse(parsed._replace(path=parsed.path[:-len("/KmlServer")]))
                q = dict(parse_qsl(parsed.query))
                layer_ids = q.get("LayerIDs") or q.get("layers") or q.get("layerIds")
                if not layer_ids:
                    continue
                ids = [s.strip() for s in layer_ids.split(",") if s.strip().isdigit()]
                if not ids:
                    continue

                for lid in ids:
                    offset = 0
                    page = 5000
                    combined_parts: list[gpd.GeoDataFrame] = []
                    while True:
                        params = {
                            "where": "1=1",
                            "outFields": "*",
                            "outSR": self.config.target_crs,
                            "f": "geojson",
                            "resultOffset": offset,
                            "resultRecordCount": page,
                        }
                        if bbox is not None:
                            params.update({
                                "geometry": f"{bbox[0]},{bbox[1]},{bbox[2]},{bbox[3]}",
                                "geometryType": "esriGeometryEnvelope",
                                "inSR": 4326,
                                "spatialRel": "esriSpatialRelIntersects",
                            })
                        query_url = f"{base_mapserver}/{lid}/query"
                        resp = requests.get(query_url, params=params, headers=headers, timeout=30)
                        resp.raise_for_status()
                        content = resp.content
                        # Save to temp geojson file
                        out_geojson = temp_dir / f"layer_{lid}_{offset}.geojson"
                        out_geojson.write_bytes(content)
                        # Read via geopandas
                        try:
                            gdf = gpd.read_file(out_geojson)
                        except Exception:
                            break
                        if gdf is None or gdf.empty:
                            break
                        combined_parts.append(gdf)
                        # If fewer than page, we are done
                        if len(gdf) < page:
                            break
                        offset += page
                    if combined_parts:
                        gdfs.append(pd.concat(combined_parts, ignore_index=True))
            except Exception:
                continue
        return gdfs

    def process_kmz(
        self,
        kmz_path: Path,
        preferred_layers: Optional[Iterable[str]] = None,
        postgis_import: bool = False
    ) -> gpd.GeoDataFrame:
        """Process KMZ file to GeoDataFrame with layer selection."""
        if not kmz_path.exists():
            raise FileNotFoundError(kmz_path)
        
        if self.config.verbose:
            print(f"Processing KMZ: {kmz_path}")
        
        # Unzip to locate .kml files
        with zipfile.ZipFile(kmz_path) as zf:
            kml_candidates = [n for n in zf.namelist() if n.lower().endswith(".kml")]
            if not kml_candidates:
                raise RuntimeError(f"No .kml found inside {kmz_path}")
            temp_dir = self.output_dir / "_tmp_kmz_extract"
            temp_dir.mkdir(parents=True, exist_ok=True)
            extracted_paths = []
            for name in kml_candidates:
                try:
                    zf.extract(name, temp_dir)
                    extracted_paths.append(temp_dir / name)
                except Exception:
                    continue

        # Resolve NetworkLinks by downloading linked KML/KMZ when possible
        all_kml_paths = list(extracted_paths)
        all_hrefs: list[str] = []
        for kml_file in list(extracted_paths):
            hrefs = self._parse_kml_network_links(kml_file)
            if hrefs:
                all_hrefs.extend(hrefs)
                linked = self._download_linked_kmls(hrefs, temp_dir=temp_dir)
                all_kml_paths.extend(linked)

        # Use config preferred layers or default
        if preferred_layers is None:
            preferred_layers = self.config.preferred_layers or []

        parts_all = []
        for kml_file in all_kml_paths:
            # Discover available layers per KML
            layers: Optional[Iterable[str]] = None
            if HAS_PYOGRIO:
                try:
                    from pyogrio import list_layers as _pyogrio_list_layers
                    layers_info = _pyogrio_list_layers(kml_file)
                    if layers_info:
                        layers = [li[0] if isinstance(li, (list, tuple)) else li for li in layers_info]
                        if self.config.verbose:
                            print(f"Pyogrio detected layers: {layers}")
                except Exception as e:
                    if self.config.verbose:
                        print(f"Pyogrio layer detection failed: {e}")
                    layers = None
            if layers is None and HAS_FIONA:
                try:
                    layers = fiona.listlayers(kml_file)
                    if self.config.verbose:
                        print(f"Fiona detected layers: {layers}")
                except Exception as e:
                    if self.config.verbose:
                        print(f"Fiona layer detection failed: {e}")
                    layers = None

            def read_layer(layer: Optional[str] = None) -> Optional[gpd.GeoDataFrame]:
                try:
                    read_kwargs = {"engine": "pyogrio"} if HAS_PYOGRIO else {}
                    if layer is not None:
                        if self.config.verbose:
                            print(f"Reading layer: {layer}")
                        return gpd.read_file(kml_file, layer=layer, **read_kwargs)
                    if self.config.verbose:
                        print("Reading default layer (no layer specified)")
                    return gpd.read_file(kml_file, **read_kwargs)
                except Exception as e:
                    if self.config.verbose:
                        print(f"Failed to read layer {layer}: {e}")
                    return None

            if layers:
                picked = next((ln for ln in preferred_layers if ln in layers), None)
                if self.config.verbose:
                    print(f"Preferred layers: {preferred_layers}")
                    print(f"Available layers: {layers}")
                    print(f"Picked layer: {picked}")
                target_layers = [picked] if picked is not None else list(layers)
                for ln in target_layers:
                    gdf_part = read_layer(ln)
                    if gdf_part is not None and not gdf_part.empty:
                        parts_all.append(gdf_part)
            else:
                if self.config.verbose:
                    print("No layers detected, trying to read default layer")
                gdf_part = read_layer()
                if gdf_part is not None and not gdf_part.empty:
                    parts_all.append(gdf_part)

        if not parts_all and all_hrefs:
            # Try ArcGIS FeatureServer/MapServer fallback
            arcgis_parts = self._fetch_arcgis_layers_from_kmlserver(all_hrefs, temp_dir=temp_dir)
            parts_all.extend(arcgis_parts)

        if not parts_all:
            raise RuntimeError(f"No readable layers in any KML within: {kmz_path}")

        gdf = pd.concat(parts_all, ignore_index=True)

        # Normalize CRS
        gdf = self.ensure_target_crs(gdf)

        # Apply bbox filter if specified
        if self.config.bbox_filter is not None:
            gdf = self.filter_bbox(gdf)
            if self.config.verbose:
                print(f"After bbox filtering: {len(gdf)} features")

        return gdf

    def process_file(
        self,
        file_path: Union[str, Path],
        lat_col: Optional[str] = None,
        lon_col: Optional[str] = None,
        preferred_layers: Optional[Iterable[str]] = None,
        return_gdf: bool = True,
        postgis_import: bool = False,
        save_outputs: bool = False,
        table_name: Optional[str] = None
    ) -> Optional[gpd.GeoDataFrame]:
        """
        Process a single file based on its extension.
        
        Args:
            file_path: Path to the file to process
            lat_col: Latitude column name for CSV files (auto-detected if None)
            lon_col: Longitude column name for CSV files (auto-detected if None)
            preferred_layers: Preferred layer names for KMZ files
            return_gdf: Whether to return GeoDataFrame
            postgis_import: Whether to import to PostGIS
            save_outputs: Whether to save processed outputs to files
            table_name: Table name for PostGIS import (auto-generated if None)
            
        Returns:
            GeoDataFrame if return_gdf=True, None otherwise
        """
        file_path = Path(file_path)
        if not file_path.exists():
            if self.config.verbose:
                print(f"File not found: {file_path}")
            return None

        suffix = file_path.suffix.lower()
        
        try:
            if suffix == '.shp':
                if self.config.verbose:
                    print(f"Processing shapefile: {file_path}")
                return self.process_shapefile(
                    file_path, 
                    return_gdf=return_gdf, 
                    postgis_import=postgis_import,
                    table_name=table_name
                )
            
            elif suffix == '.kmz':
                if self.config.verbose:
                    print(f"Processing KMZ: {file_path}")
                gdf = self.process_kmz(file_path, preferred_layers=preferred_layers, postgis_import=postgis_import)
                if postgis_import and self.db_config:
                    # For KMZ files, we need to handle PostGIS import differently since they don't go through process_shapefile
                    if table_name is None:
                        table_name = self.sanitize_table_name(file_path.stem)
                    self._import_to_postgis(gdf, table_name)
                elif postgis_import and not self.db_config:
                    if self.config.verbose:
                        print("Warning: PostGIS import requested but no database configuration provided. Skipping import.")
                if save_outputs:
                    basename = self.sanitize_name(file_path.stem)
                    self.write_outputs(gdf, basename)
                    if self.config.verbose:
                        print(f"Saved outputs: {basename}")
                return gdf if return_gdf else None
            
            elif suffix == '.csv':
                if self.config.verbose:
                    print(f"Processing CSV: {file_path}")
                gdf = self.process_csv_to_points(file_path, lat_col, lon_col, postgis_import=postgis_import)
                if postgis_import and self.db_config:
                    # For CSV files, we need to handle PostGIS import differently since they don't go through process_shapefile
                    if table_name is None:
                        table_name = self.sanitize_table_name(file_path.stem)
                    self._import_to_postgis(gdf, table_name)
                elif postgis_import and not self.db_config:
                    if self.config.verbose:
                        print("Warning: PostGIS import requested but no database configuration provided. Skipping import.")
                if save_outputs:
                    basename = self.sanitize_name(file_path.stem)
                    self.write_outputs(gdf, basename)
                    if self.config.verbose:
                        print(f"Saved outputs: {basename}")
                return gdf if return_gdf else None
            
            else:
                if self.config.verbose:
                    print(f"Unsupported file type: {suffix}")
                return None
                
        except Exception as e:
            if self.config.verbose:
                print(f"Error processing {file_path}: {e}")
            return None

    def process_batch(
        self,
        file_paths: Iterable[Union[str, Path]],
        lat_col: Optional[str] = None,
        lon_col: Optional[str] = None,
        preferred_layers: Optional[Iterable[str]] = None,
        return_gdfs: bool = True,
        postgis_import: bool = False,
        save_outputs: bool = False
    ) -> Dict[str, Optional[gpd.GeoDataFrame]]:
        """
        Process multiple files in batch.
        
        Returns:
            Dictionary mapping file paths to processed GeoDataFrames (or None if failed)
        """
        results = {}
        
        for file_path in file_paths:
            if self.config.verbose:
                print(f"\n=== Processing {file_path} ===")
            
            try:
                gdf = self.process_file(
                    file_path, 
                    lat_col=lat_col,
                    lon_col=lon_col,
                    preferred_layers=preferred_layers,
                    return_gdf=return_gdfs,
                    postgis_import=postgis_import,
                    save_outputs=save_outputs,
                    table_name=None  # Will auto-generate table name
                )
                results[str(file_path)] = gdf
                
                if gdf is not None and self.config.verbose:
                    print(f"Successfully processed {file_path} -> {len(gdf)} features")
                elif self.config.verbose:
                    print(f"Processed {file_path} (no GeoDataFrame returned)")
                    
            except Exception as e:
                if self.config.verbose:
                    print(f"Error processing {file_path}: {e}")
                results[str(file_path)] = None
        
        return results

    def import_to_postgis(
        self,
        file_path: Union[str, Path],
        table_name: Optional[str] = None,
        db_config: Optional[DatabaseConfig] = None
    ) -> bool:
        """Import a file to PostGIS database."""
        if not db_config and not self.db_config:
            if self.config.verbose:
                print("Error: Database configuration required for PostGIS import")
            return False
        
        db_config = db_config or self.db_config
        file_path = Path(file_path)
        
        try:
            self.process_file(
                file_path,
                return_gdf=False,
                postgis_import=True,
                table_name=table_name
            )
            return True
        except Exception as e:
            if self.config.verbose:
                print(f"Failed to import {file_path} to PostGIS: {e}")
            return False


def main():
    """Command line interface for the spatial data processor."""
    parser = argparse.ArgumentParser(description="Generalized spatial data preprocessing pipeline")
    parser.add_argument("files", nargs="*", help="Files to process (shapefiles, KMZ, CSV)")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose output")
    parser.add_argument("--postgis", action="store_true", help="Import to PostGIS database")
    parser.add_argument("--save-outputs", action="store_true", help="Save processed outputs to files")
    parser.add_argument("--target-crs", type=int, default=4326, help="Target CRS EPSG code (default: 4326)")
    parser.add_argument("--bbox", nargs=4, type=float, metavar=("min_lon", "min_lat", "max_lon", "max_lat"), 
                       help="Bounding box filter: min_lon min_lat max_lon max_lat")
    parser.add_argument("--output-dir", default="outputs", help="Output directory (default: outputs)")
    parser.add_argument("--db-host", default="localhost", help="Database host (default: localhost)")
    parser.add_argument("--db-port", type=int, default=5432, help="Database port (default: 5432)")
    parser.add_argument("--db-user", default="postgres", help="Database user (default: postgres)")
    parser.add_argument("--db-password", default="", help="Database password")
    parser.add_argument("--db-name", default="spatial_db", help="Database name (default: spatial_db)")
    
    args = parser.parse_args()
    
    # Create configuration
    config = ProcessingConfig(
        target_crs=args.target_crs,
        bbox_filter=tuple(args.bbox) if args.bbox else None,
        verbose=args.verbose
    )
    
    # Create database config if needed
    db_config = None
    if args.postgis:
        db_config = DatabaseConfig(
            host=args.db_host,
            port=args.db_port,
            user=args.db_user,
            password=args.db_password,
            database=args.db_name
        )
    
    # Create processor
    processor = SpatialDataProcessor(
        config=config,
        output_dir=args.output_dir,
        db_config=db_config
    )
    
    if args.files:
        # Process specified files
        file_paths = [Path(f) for f in args.files]
        results = processor.process_batch(
            file_paths, 
            return_gdfs=not args.postgis,
            postgis_import=args.postgis,
            save_outputs=args.save_outputs
        )
        
        if args.verbose:
            print(f"\n=== Processing Summary ===")
            for file_path, result in results.items():
                status = "✓ Success" if result is not None else "✗ Failed"
                print(f"{file_path}: {status}")
    else:
        print("No files specified. Use --help for usage information.")


if __name__ == "__main__":
    main()




