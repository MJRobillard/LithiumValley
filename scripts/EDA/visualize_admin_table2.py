"""
Visualize admin_table2 from PostgreSQL on an interactive map.

This script creates a live connection to the PostgreSQL database and displays
the admin_table2 geometries on an interactive web map using Folium.

Usage:
    python scripts/visualize_admin_table2.py
"""

import os
import sys
from pathlib import Path
import geopandas as gpd
import pandas as pd
from sqlalchemy import create_engine, text
import folium
from folium.plugins import MarkerCluster, HeatMap
import webbrowser
import tempfile

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


def create_interactive_map(gdf: gpd.GeoDataFrame, output_path: str = None) -> folium.Map:
    """Create an interactive Folium map with the admin_table2 data."""
    
    # Calculate center of the data
    bounds = gdf.total_bounds
    center_lat = (bounds[1] + bounds[3]) / 2
    center_lon = (bounds[0] + bounds[2]) / 2
    
    # Create base map
    m = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=8,
        tiles='OpenStreetMap'
    )
    
    # Add different tile layers with proper attribution
    folium.TileLayer(
        tiles='Stamen Terrain',
        name='Terrain',
        attr='Map tiles by <a href="http://stamen.com">Stamen Design</a>, under <a href="http://creativecommons.org/licenses/by/3.0">CC BY 3.0</a>. Data by <a href="http://openstreetmap.org">OpenStreetMap</a>, under <a href="http://www.openstreetmap.org/copyright">ODbL</a>.'
    ).add_to(m)
    
    folium.TileLayer(
        tiles='Stamen Toner',
        name='Toner',
        attr='Map tiles by <a href="http://stamen.com">Stamen Design</a>, under <a href="http://creativecommons.org/licenses/by/3.0">CC BY 3.0</a>. Data by <a href="http://openstreetmap.org">OpenStreetMap</a>, under <a href="http://www.openstreetmap.org/copyright">ODbL</a>.'
    ).add_to(m)
    
    folium.TileLayer(
        tiles='CartoDB positron',
        name='Positron',
        attr='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors &copy; <a href="https://carto.com/attributions">CARTO</a>'
    ).add_to(m)
    
    # Get non-geometry columns for dynamic feature grouping
    geom_col = gdf.geometry.name
    data_columns = [col for col in gdf.columns if col != geom_col]
    
    print(f"Dynamic columns detected: {data_columns}")
    print(f"Geometry column: {geom_col}")
    
    # Create dynamic feature groups based on column values
    feature_groups = {}
    color_palette = ['blue', 'green', 'purple', 'orange', 'red', 'brown', 'pink', 'gray', 'olive', 'cyan']
    
    # Analyze each column to create feature groups
    for col in data_columns:
        if col == 'id':  # Skip ID columns
            continue
            
        unique_values = gdf[col].dropna().unique()
        if len(unique_values) <= 20:  # Only group if reasonable number of unique values
            print(f"Creating feature group for column '{col}' with {len(unique_values)} unique values")
            
            # Create a feature group for this column
            group_name = f"{col.replace('_', ' ').title()}"
            feature_groups[col] = {
                'group': folium.FeatureGroup(name=group_name, show=True),
                'values': unique_values,
                'colors': {}
            }
            
            # Assign colors to unique values
            for i, value in enumerate(unique_values):
                if pd.isna(value) or value == '':
                    continue
                color_idx = i % len(color_palette)
                feature_groups[col]['colors'][value] = color_palette[color_idx]
    
    # Add geometries to map based on dynamic grouping
    for idx, row in gdf.iterrows():
        # Create dynamic popup content based on available columns
        popup_content = ""
        for col in data_columns:
            value = row.get(col, 'N/A')
            if pd.notna(value) and value != '':
                popup_content += f"<b>{col.replace('_', ' ').title()}:</b> {value}<br>"
        
        # Determine which feature group to use and styling
        added_to_group = False
        
        for col, group_info in feature_groups.items():
            value = row.get(col)
            if pd.notna(value) and value != '' and value in group_info['colors']:
                color = group_info['colors'][value]
                
                # Create tooltip
                tooltip = f"{col.replace('_', ' ').title()}: {value}"
                
                # Add geometry to this feature group
                folium.GeoJson(
                    row.geom,
                    name=f"{col}: {value}",
                    style_function=lambda x, fill_color=color: {
                        'fillColor': fill_color,
                        'color': 'black',
                        'weight': 1,
                        'fillOpacity': 0.6
                    },
                    popup=folium.Popup(popup_content, max_width=300),
                    tooltip=tooltip
                ).add_to(group_info['group'])
                
                added_to_group = True
                break
        
        # If no specific group found, add to a default group
        if not added_to_group:
            if 'default' not in feature_groups:
                feature_groups['default'] = {
                    'group': folium.FeatureGroup(name='Other Features', show=True),
                    'values': [],
                    'colors': {}
                }
            
            folium.GeoJson(
                row.geom,
                name=f"Feature {idx}",
                style_function=lambda x: {
                    'fillColor': 'gray',
                    'color': 'black',
                    'weight': 1,
                    'fillOpacity': 0.4
                },
                popup=folium.Popup(popup_content, max_width=300),
                tooltip=f"Feature {idx}"
            ).add_to(feature_groups['default']['group'])
    
    # Add all feature groups to map
    for col, group_info in feature_groups.items():
        group_info['group'].add_to(m)
        print(f"Added feature group: {group_info['group'].name} with {len(group_info['values'])} unique values")
    
    # Add layer control
    folium.LayerControl().add_to(m)
    
    # Add fullscreen option
    folium.plugins.Fullscreen().add_to(m)
    
    # Add measure tool
    folium.plugins.MeasureControl().add_to(m)
    
    # Fit map to data bounds
    m.fit_bounds(gdf.total_bounds.tolist())
    
    return m


def save_and_open_map(m: folium.Map, output_path: str = None) -> str:
    """Save the map and open it in browser."""
    if output_path is None:
        output_path = "outputs/admin_table2_map.html"
    
    # Ensure output directory exists
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    
    # Save map
    m.save(output_path)
    print(f"Map saved to: {output_path}")
    
    # Open in browser
    try:
        webbrowser.open(f'file://{os.path.abspath(output_path)}')
        print("Map opened in browser")
    except Exception as e:
        print(f"Could not open browser automatically: {e}")
        print(f"Please open {output_path} manually in your browser")
    
    return output_path


def main():
    """Main function to load data and create map."""
    print("=== Admin Table 2 Map Visualization ===")
    
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
        
        # Create interactive map
        print("\nCreating interactive map...")
        m = create_interactive_map(gdf)
        
        # Save and open map
        output_path = save_and_open_map(m)
        
        print(f"\n✅ Map visualization complete!")
        print(f"Map file: {output_path}")
        print("\nFeatures:")
        print("- Dynamic feature grouping based on your data columns")
        print("- Automatic color assignment for different values")
        print("- Click on features for detailed information")
        print("- Use layer control to toggle different feature types")
        print("- Fullscreen and measurement tools available")
        print("- Geometry column automatically detected and handled")
        
    except Exception as e:
        print(f"Error: {e}")
        print("\nTroubleshooting:")
        print("1. Ensure PostgreSQL is running")
        print("2. Check connection parameters in the script")
        print("3. Verify admin_table2 exists in the database")
        sys.exit(1)


if __name__ == "__main__":
    main()
