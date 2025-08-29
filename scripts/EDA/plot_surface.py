"""
Plot surface layers produced by preprocessing into static PNG maps.

Outputs are saved under `data/processed/surface/plots/`:
- Combined overlay map of all layers
- Individual maps for each layer

Dependencies:
  pip install geopandas matplotlib shapely pyproj
Optional:
  pip install contextily  # for web basemap (slippy tiles)

Run:
  python scripts/plot_surface.py

You can edit SALTON_TROUGH_BBOX to control the default extent.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Dict, Optional, Tuple

import geopandas as gpd
import matplotlib.pyplot as plt

# Optional basemap (install contextily to enable web tiles). It's okay if unavailable.
try:  # pragma: no cover - optional
    import contextily as cx  # type: ignore
    HAS_CONTEXTILY = True
except Exception:
    HAS_CONTEXTILY = False

REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = REPO_ROOT / "data"
PROC_DIR = DATA_DIR / "processed" / "surface"
PLOTS_DIR = PROC_DIR / "plots"
PLOTS_DIR.mkdir(parents=True, exist_ok=True)

# Default focus extent: Southern California (coarse)
SALTON_TROUGH_BBOX: Optional[Tuple[Tuple[float, float], Tuple[float, float]]] = (
    (-121.0, 32.0),  # (min_lon, min_lat)
    (-114.0, 35.9),  # (max_lon, max_lat)
)

# Layer file mapping
LAYER_FILES: Dict[str, Path] = {
    "USGS_Li_map": PROC_DIR / "USGS_Li_map.shp",
    "GEOTHERM_points": PROC_DIR / "GEOTHERM_points.shp",
    "BLM_CA_Geothermal_Leases": PROC_DIR / "BLM_CA_Geothermal_Leases.shp",
    "Federal_American_Indian_Reservations": PROC_DIR / "Federal_American_Indian_Reservations.shp",
}


def ensure_crs(gdf: gpd.GeoDataFrame, epsg: int) -> gpd.GeoDataFrame:
    if gdf.crs is None:
        gdf = gdf.set_crs(4326)
    if gdf.crs.to_epsg() != epsg:
        gdf = gdf.to_crs(epsg)
    return gdf


def get_combined_bounds(layers: Dict[str, gpd.GeoDataFrame]):
    total = None
    for gdf in layers.values():
        if gdf.empty:
            continue
        if total is None:
            total = gdf.total_bounds
        else:
            xmin, ymin, xmax, ymax = total
            gxmin, gymin, gxmax, gymax = gdf.total_bounds
            total = (min(xmin, gxmin), min(ymin, gymin), max(xmax, gxmax), max(ymax, gymax))
    return total


def apply_bbox(ax, bbox4326: Tuple[Tuple[float, float], Tuple[float, float]], use_web_mercator: bool):
    (min_lon, min_lat), (max_lon, max_lat) = bbox4326
    if use_web_mercator:
        import pyproj
        proj = pyproj.Transformer.from_crs(4326, 3857, always_xy=True)
        x0, y0 = proj.transform(min_lon, min_lat)
        x1, y1 = proj.transform(max_lon, max_lat)
        ax.set_xlim(x0, x1)
        ax.set_ylim(y0, y1)
    else:
        ax.set_xlim(min_lon, max_lon)
        ax.set_ylim(min_lat, max_lat)


def plot_layers():
    # Load layers that exist
    loaded: Dict[str, gpd.GeoDataFrame] = {}
    for name, path in LAYER_FILES.items():
        if path.exists():
            gdf = gpd.read_file(path)
            loaded[name] = gdf
        else:
            print(f"Warning: layer not found, skipping: {path}")

    # Discover SoCal SMA shapefiles (created by extract_socal_sma.py)
    socal_layers: Dict[str, gpd.GeoDataFrame] = {}
    for shp in sorted(PROC_DIR.glob("*SoCal.shp")):
        name = shp.stem  # e.g., SurfaceMgtAgy_BLM_SoCal
        try:
            gdf = gpd.read_file(shp)
            socal_layers[name] = gdf
        except Exception as e:
            print(f"Warning: failed to read {shp}: {e}")

    if not loaded:
        print("No base layers found. Did you run the preprocessing script?")
        return

    # Individual plots (WGS84)
    for name, gdf in loaded.items():
        if gdf is None or gdf.empty:
            print(f"Skipping empty layer: {name}")
            continue
        fig, ax = plt.subplots(figsize=(10, 10))
        gdf = ensure_crs(gdf, 4326)
        # Symbology per layer type
        if name == "GEOTHERM_points":
            # points
            gdf.plot(ax=ax, color="#1f77b4", markersize=5, alpha=0.8, aspect="auto")
        elif name == "USGS_Li_map":
            gdf.plot(ax=ax, color="#e74c3c", alpha=0.4, edgecolor="#c0392b", linewidth=0.5, aspect="auto")
        elif name == "BLM_CA_Geothermal_Leases":
            gdf.plot(ax=ax, color="#f39c12", alpha=0.35, edgecolor="#d35400", linewidth=0.5, aspect="auto")
        elif name == "Federal_American_Indian_Reservations":
            gdf.plot(ax=ax, color="#8e44ad", alpha=0.25, edgecolor="#6c3483", linewidth=0.5, aspect="auto")
        else:
            gdf.plot(ax=ax, color="#7f8c8d", alpha=0.3, edgecolor="#2c3e50", linewidth=0.5, aspect="auto")

        ax.set_title(name)
        ax.set_xlabel("Longitude (°)")
        ax.set_ylabel("Latitude (°)")

        if SALTON_TROUGH_BBOX is not None:
            apply_bbox(ax, SALTON_TROUGH_BBOX, use_web_mercator=False)

        fig.tight_layout()
        out = PLOTS_DIR / f"{name}.png"
        fig.savefig(out, dpi=200)
        plt.close(fig)
        print(f"Saved {out}")

    # Combined overlay with optional basemap (Web Mercator)
    fig, ax = plt.subplots(figsize=(12, 12))

    # Reproject to Web Mercator for basemap compatibility
    reprojected: Dict[str, gpd.GeoDataFrame] = {name: ensure_crs(gdf, 3857) for name, gdf in loaded.items()}

    # Draw in order (polygons first, then points)
    for name in [
        "Federal_American_Indian_Reservations",
        "BLM_CA_Geothermal_Leases",
        "USGS_Li_map",
        "GEOTHERM_points",
    ]:
        gdf = reprojected.get(name)
        if gdf is None or gdf.empty:
            continue
        if name == "GEOTHERM_points":
            gdf.plot(ax=ax, color="#1f77b4", markersize=5, alpha=0.9, label=name, aspect="auto")
        elif name == "USGS_Li_map":
            gdf.plot(ax=ax, color="#e74c3c", alpha=0.35, edgecolor="#c0392b", linewidth=0.5, label=name, aspect="auto")
        elif name == "BLM_CA_Geothermal_Leases":
            gdf.plot(ax=ax, color="#f39c12", alpha=0.30, edgecolor="#d35400", linewidth=0.4, label=name, aspect="auto")
        elif name == "Federal_American_Indian_Reservations":
            gdf.plot(ax=ax, color="#8e44ad", alpha=0.20, edgecolor="#6c3483", linewidth=0.4, label=name, aspect="auto")

    # Basemap
    if HAS_CONTEXTILY:
        try:
            cx.add_basemap(ax, source=cx.providers.CartoDB.PositronNoLabels, attribution_size=6)
        except Exception as e:
            print(f"Basemap failed: {e}")

    # Apply extent
    if SALTON_TROUGH_BBOX is not None:
        apply_bbox(ax, SALTON_TROUGH_BBOX, use_web_mercator=True)
    else:
        bounds = get_combined_bounds(reprojected)
        if bounds is not None:
            xmin, ymin, xmax, ymax = bounds
            pad_x = (xmax - xmin) * 0.05
            pad_y = (ymax - ymin) * 0.05
            ax.set_xlim(xmin - pad_x, xmax + pad_x)
            ax.set_ylim(ymin - pad_y, ymax + pad_y)

    ax.set_axis_off()
    ax.legend(loc="lower left")
    fig.tight_layout()

    out = PLOTS_DIR / "overlay.png"
    fig.savefig(out, dpi=220)
    plt.close(fig)
    print(f"Saved {out}")

    # Plot SoCal SMA layers individually
    if socal_layers:
        color_cycle = [
            "#1abc9c", "#3498db", "#9b59b6", "#e67e22", "#e74c3c",
            "#2ecc71", "#f1c40f", "#34495e", "#16a085", "#8e44ad",
        ]
        for idx, (name, gdf) in enumerate(socal_layers.items()):
            if gdf is None or gdf.empty:
                print(f"Skipping empty SoCal layer: {name}")
                continue
            fig, ax = plt.subplots(figsize=(10, 10))
            gdf = ensure_crs(gdf, 4326)
            color = color_cycle[idx % len(color_cycle)]
            gdf.plot(ax=ax, color=color, alpha=0.3, edgecolor="#2c3e50", linewidth=0.4, aspect="auto")
            ax.set_title(name)
            ax.set_xlabel("Longitude (°)")
            ax.set_ylabel("Latitude (°)")
            if SALTON_TROUGH_BBOX is not None:
                apply_bbox(ax, SALTON_TROUGH_BBOX, use_web_mercator=False)
            fig.tight_layout()
            out = PLOTS_DIR / f"{name}.png"
            fig.savefig(out, dpi=200)
            plt.close(fig)
            print(f"Saved {out}")

        # Combined overlay for SoCal SMA layers
        fig, ax = plt.subplots(figsize=(12, 12))
        reprojected_socal: Dict[str, gpd.GeoDataFrame] = {n: ensure_crs(g, 3857) for n, g in socal_layers.items()}
        for idx, (name, gdf) in enumerate(reprojected_socal.items()):
            if gdf is None or gdf.empty:
                continue
            color = color_cycle[idx % len(color_cycle)]
            gdf.plot(ax=ax, color=color, alpha=0.28, edgecolor="#2c3e50", linewidth=0.3, label=name, aspect="auto")
        if HAS_CONTEXTILY:
            try:
                cx.add_basemap(ax, source=cx.providers.CartoDB.PositronNoLabels, attribution_size=6)
            except Exception as e:
                print(f"Basemap failed (SoCal overlay): {e}")
        if SALTON_TROUGH_BBOX is not None:
            apply_bbox(ax, SALTON_TROUGH_BBOX, use_web_mercator=True)
        ax.set_axis_off()
        ax.legend(loc="lower left", fontsize=8)
        fig.tight_layout()
        out = PLOTS_DIR / "SMA_SoCal_overlay.png"
        fig.savefig(out, dpi=220)
        plt.close(fig)
        print(f"Saved {out}")


if __name__ == "__main__":
    plot_layers()
