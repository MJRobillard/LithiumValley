"""
Visualize admin_table2 from PostgreSQL using matplotlib.

This script creates a live connection to the PostgreSQL database and displays
the admin_table2 geometries on a static map using matplotlib.

Usage:
    python scripts/visualize_admin_table2_matplotlib.py
"""

import os
import sys
from pathlib import Path
import geopandas as gpd
import pandas as pd
from sqlalchemy import create_engine, text
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.colors import ListedColormap
import numpy as np

# Default connection parameters (aligned with export script)
DEFAULT_HOST = "localhost"
DEFAULT_PORT = 5432
DEFAULT_USER = "postgres"
DEFAULT_PASSWORD = "1123"
DEFAULT_DB = "lithiumvalley"


def build_connection_url(host: str, port: int, db: str, user: str, password: str) -> str:
    """Build PostgreSQL connection URL."""
    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"


def load_admin_table2(host: str, port: int, db: str, user: str, password: str) -> gpd.GeoDataFrame:
    """Load admin_table2 from PostgreSQL."""
    engine = create_engine(build_connection_url(host, port, db, user, password))
    
    print("Connecting to PostgreSQL...")
    print(f"Database: {db} on {host}:{port}")
    
    # Load the data
    sql = """
    SELECT id, blm_id, blm_admin, res_name, geom
    FROM public.admin_table2
    WHERE geom IS NOT NULL AND NOT ST_IsEmpty(geom)
    """
    
    print("Loading admin_table2 data...")
    with engine.begin() as conn:
        gdf = gpd.read_postgis(sql, conn, geom_col='geom')
    
    print(f"Loaded {len(gdf)} features")
    print(f"CRS: {gdf.crs}")
    
    # Display sample data
    print("\nSample data:")
    print(gdf.head())
    
    # Show unique values in key columns
    if 'blm_admin' in gdf.columns:
        print(f"\nUnique blm_admin values: {gdf['blm_admin'].unique()}")
    if 'res_name' in gdf.columns:
        print(f"Unique res_name values: {gdf['res_name'].dropna().unique()}")
    
    return gdf


def create_matplotlib_map(gdf: gpd.GeoDataFrame, output_path: str = None) -> None:
    """Create a matplotlib map with the admin_table2 data."""
    
    if output_path is None:
        output_path = "outputs/admin_table2_map.png"
    
    # Ensure output directory exists
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    
    # Create figure and axis
    fig, ax = plt.subplots(1, 1, figsize=(15, 12))
    
    # Color mapping for blm_admin
    admin_colors = {
        'BLM': '#1f77b4',      # blue
        'USFS': '#2ca02c',     # green
        'NPS': '#9467bd',      # purple
        'FWS': '#ff7f0e',      # orange
        'DOD': '#d62728',      # red
        'Other': '#7f7f7f'     # gray
    }
    
    # Separate BLM areas and reservations
    blm_areas = gdf[gdf['res_name'].isna() | (gdf['res_name'] == '')]
    reservations = gdf[gdf['res_name'].notna() & (gdf['res_name'] != '')]
    
    print(f"Plotting {len(blm_areas)} BLM areas and {len(reservations)} reservations")
    
    # Plot BLM areas
    for idx, row in blm_areas.iterrows():
        admin = row.get('blm_admin', 'Other')
        color = admin_colors.get(admin, admin_colors['Other'])
        
        # Plot the geometry
        gdf_plot = gpd.GeoDataFrame([row], geometry='geom', crs=gdf.crs)
        gdf_plot.plot(
            ax=ax,
            color=color,
            edgecolor='black',
            linewidth=0.5,
            alpha=0.6,
            label=f"BLM: {admin}"
        )
    
    # Plot reservations
    if not reservations.empty:
        reservation_gdf = gpd.GeoDataFrame(reservations, geometry='geom', crs=gdf.crs)
        reservation_gdf.plot(
            ax=ax,
            color='yellow',
            edgecolor='orange',
            linewidth=2,
            alpha=0.8,
            label='Reservations'
        )
    
    # Customize the map
    ax.set_title('Admin Table 2: BLM Areas and Reservations', fontsize=16, fontweight='bold')
    ax.set_xlabel('Longitude')
    ax.set_ylabel('Latitude')
    
    # Add grid
    ax.grid(True, alpha=0.3)
    
    # Create legend
    legend_elements = []
    
    # Add BLM admin types to legend
    for admin, color in admin_colors.items():
        if admin in blm_areas['blm_admin'].values:
            legend_elements.append(mpatches.Patch(color=color, label=f'BLM: {admin}'))
    
    # Add reservations to legend if they exist
    if not reservations.empty:
        legend_elements.append(mpatches.Patch(color='yellow', edgecolor='orange', 
                                           label='Reservations', linewidth=2))
    
    ax.legend(handles=legend_elements, loc='upper right', bbox_to_anchor=(1.15, 1))
    
    # Add text box with statistics
    stats_text = f"""
    Total Features: {len(gdf)}
    BLM Areas: {len(blm_areas)}
    Reservations: {len(reservations)}
    CRS: {gdf.crs}
    """
    
    ax.text(0.02, 0.98, stats_text, transform=ax.transAxes, fontsize=10,
            verticalalignment='top', bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))
    
    # Adjust layout and save
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"Map saved to: {output_path}")
    
    # Show the plot
    plt.show()


def main():
    """Main function to load data and create map."""
    print("=== Admin Table 2 Matplotlib Visualization ===")
    
    try:
        # Load data from PostgreSQL
        gdf = load_admin_table2(
            host=DEFAULT_HOST,
            port=DEFAULT_PORT,
            db=DEFAULT_DB,
            user=DEFAULT_USER,
            password=DEFAULT_PASSWORD
        )
        
        if gdf.empty:
            print("No data found in admin_table2")
            return
        
        # Create matplotlib map
        print("\nCreating matplotlib map...")
        create_matplotlib_map(gdf)
        
        print(f"\n✅ Map visualization complete!")
        print("\nFeatures:")
        print("- BLM areas are colored by administrative agency")
        print("- Reservations are highlighted in yellow/orange")
        print("- Legend shows all administrative types present in the data")
        print("- Statistics box shows feature counts and CRS information")
        
    except Exception as e:
        print(f"Error: {e}")
        print("\nTroubleshooting:")
        print("1. Ensure PostgreSQL is running")
        print("2. Check connection parameters in the script")
        print("3. Verify admin_table2 exists in the database")
        print("4. Ensure matplotlib is properly installed")
        sys.exit(1)


if __name__ == "__main__":
    main()
