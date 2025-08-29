#!/usr/bin/env python3
"""
Visualize the Tableau-ready geologic data to show the improved geometries.
"""

import geopandas as gpd
import matplotlib.pyplot as plt
from pathlib import Path
import numpy as np

def visualize_tableau_geology():
    """Visualize the new Tableau-ready geologic data."""
    
    # Load the new Tableau-ready data
    tableau_shapefile = Path("outputs/CGS_Geologic_Map_Salton_Trough_Tableau.shp")
    
    if not tableau_shapefile.exists():
        print("Tableau shapefile not found. Please run the Tableau-ready script first.")
        return
    
    print("Loading Tableau-ready geologic data...")
    gdf_tableau = gpd.read_file(tableau_shapefile)
    
    # Create a figure with multiple subplots
    fig, axes = plt.subplots(2, 2, figsize=(20, 16))
    fig.suptitle('Salton Trough Geologic Map - Tableau Ready vs Previous Versions', fontsize=16)
    
    # Plot 1: Tableau-ready data with geologic units
    ax1 = axes[0, 0]
    gdf_tableau.plot(
        column='UNIT_NAME',
        ax=ax1,
        legend=True,
        legend_kwds={'loc': 'upper left', 'bbox_to_anchor': (1, 1)},
        cmap='Set3'
    )
    ax1.set_title('Tableau-Ready Geologic Map\n(Clean Geometries)')
    ax1.set_xlabel('Longitude')
    ax1.set_ylabel('Latitude')
    
    # Plot 2: Tableau-ready data with rock types
    ax2 = axes[0, 1]
    gdf_tableau.plot(
        column='ROCK_TYPE',
        ax=ax2,
        legend=True,
        legend_kwds={'loc': 'upper left', 'bbox_to_anchor': (1, 1)},
        cmap='tab10'
    )
    ax2.set_title('Rock Types Distribution')
    ax2.set_xlabel('Longitude')
    ax2.set_ylabel('Latitude')
    
    # Plot 3: Age distribution
    ax3 = axes[1, 0]
    gdf_tableau.plot(
        column='AGE_MA',
        ax=ax3,
        legend=True,
        cmap='viridis'
    )
    ax3.set_title('Geologic Age (Millions of Years)')
    ax3.set_xlabel('Longitude')
    ax3.set_ylabel('Latitude')
    
    # Plot 4: Area distribution
    ax4 = axes[1, 1]
    gdf_tableau.plot(
        column='AREA_KM2',
        ax=ax4,
        legend=True,
        cmap='plasma'
    )
    ax4.set_title('Area Distribution (km²)')
    ax4.set_xlabel('Longitude')
    ax4.set_ylabel('Latitude')
    
    # Adjust layout
    plt.tight_layout()
    
    # Save the plot
    output_path = Path("outputs/tableau_geology_visualization.png")
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"Visualization saved to: {output_path}")
    
    # Display summary statistics
    print(f"\n=== TABLEAU-READY GEOLOGIC DATA SUMMARY ===")
    print(f"Total features: {len(gdf_tableau)}")
    print(f"Geologic units: {gdf_tableau['UNIT_NAME'].nunique()}")
    print(f"Rock types: {gdf_tableau['ROCK_TYPE'].nunique()}")
    print(f"Age range: {gdf_tableau['AGE_MA'].min():.1f} - {gdf_tableau['AGE_MA'].max():.1f} Ma")
    print(f"Area range: {gdf_tableau['AREA_KM2'].min():.1f} - {gdf_tableau['AREA_KM2'].max():.1f} km²")
    
    # Show unique geologic units
    print(f"\nUnique Geologic Units:")
    unit_counts = gdf_tableau['UNIT_NAME'].value_counts()
    for unit, count in unit_counts.items():
        print(f"  {unit}: {count} features")
    
    # Show geometry quality metrics
    print(f"\n=== GEOMETRY QUALITY METRICS ===")
    
    # Check for valid geometries
    valid_geoms = gdf_tableau.geometry.is_valid.sum()
    print(f"Valid geometries: {valid_geoms}/{len(gdf_tableau)}")
    
    # Check for simple geometries
    simple_geoms = gdf_tableau.geometry.apply(lambda g: g.is_simple).sum()
    print(f"Simple geometries: {simple_geoms}/{len(gdf_tableau)}")
    
    # Check for closed geometries
    closed_geoms = gdf_tableau.geometry.apply(lambda g: g.is_closed).sum()
    print(f"Closed geometries: {closed_geoms}/{len(gdf_tableau)}")
    
    # Show coordinate counts (should be reasonable for Tableau)
    coord_counts = gdf_tableau.geometry.apply(lambda g: len(g.exterior.coords))
    print(f"Coordinate counts per polygon: {coord_counts.min()} - {coord_counts.max()}")
    
    # Show the plot
    plt.show()
    
    return gdf_tableau

def compare_with_previous_versions():
    """Compare the new data with previous versions if available."""
    
    print(f"\n=== COMPARISON WITH PREVIOUS VERSIONS ===")
    
    # Check what previous files exist
    output_dir = Path("outputs")
    existing_files = list(output_dir.glob("*.shp"))
    
    print(f"Available shapefiles:")
    for file in existing_files:
        file_size = file.stat().st_size / 1024  # KB
        print(f"  {file.name}: {file_size:.1f} KB")
    
    # Try to load and compare if previous versions exist
    comparison_files = [
        "CGS_Geologic_Map_Salton_Trough.shp",
        "CGS_Geologic_Map_Salton_Trough_Comprehensive.shp"
    ]
    
    for filename in comparison_files:
        file_path = output_dir / filename
        if file_path.exists():
            try:
                gdf_prev = gpd.read_file(file_path)
                print(f"\n{filename}:")
                print(f"  Features: {len(gdf_prev)}")
                print(f"  Columns: {len(gdf_prev.columns)}")
                print(f"  Geometry types: {gdf_prev.geometry.geom_type.unique()}")
                
                # Check if it has geologic information
                if 'UNIT_NAME' in gdf_prev.columns:
                    print(f"  Geologic units: {gdf_prev['UNIT_NAME'].nunique()}")
                else:
                    print(f"  No geologic unit information")
                    
            except Exception as e:
                print(f"  Error loading {filename}: {e}")

if __name__ == "__main__":
    # Visualize the new Tableau-ready data
    gdf_tableau = visualize_tableau_geology()
    
    # Compare with previous versions
    compare_with_previous_versions()
    
    print(f"\n=== RECOMMENDATIONS FOR TABLEAU ===")
    print(f"1. Use the new 'CGS_Geologic_Map_Salton_Trough_Tableau.shp' file")
    print(f"2. The geometries are now clean, simple polygons optimized for Tableau")
    print(f"3. Each feature has comprehensive geologic attributes for analysis")
    print(f"4. Color codes are included for consistent mapping")
    print(f"5. Multiple export formats available (Shapefile, GeoJSON, CSV)")
