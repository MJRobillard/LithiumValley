import geopandas as gpd
import matplotlib.pyplot as plt
import os
from pathlib import Path
import numpy as np
from shapely.geometry import box

def check_spatial_intersection(data_dir="data/BLM_Natl_Rights-of-Way_Planning_Tool_Energy_Designations_Group_Feature_Layer", 
                              surface_dir="data/processed/surface"):
    """
    Check if BLM infrastructure data intersects with the Salton Trough study area.
    Analyze coordinate systems and spatial relationships.
    """
    
    # Salton Trough bounding box coordinates (WGS84)
    SALTON_TROUGH_BBOX = {
        'min_lon': -116.688,
        'min_lat': 32.64,
        'max_lon': -114.472,
        'max_lat': 34.034
    }
    
    print("="*80)
    print("SPATIAL INTERSECTION ANALYSIS")
    print("="*80)
    
    # Create Salton Trough bounding box in WGS84
    salton_bbox = box(SALTON_TROUGH_BBOX['min_lon'], SALTON_TROUGH_BBOX['min_lat'],
                      SALTON_TROUGH_BBOX['max_lon'], SALTON_TROUGH_BBOX['max_lat'])
    salton_gdf = gpd.GeoDataFrame(geometry=[salton_bbox], crs='EPSG:4326')
    
    print(f"Salton Trough Study Area:")
    print(f"  Bounding Box: {SALTON_TROUGH_BBOX}")
    print(f"  CRS: {salton_gdf.crs}")
    print()
    
    # Check surface management data (should be in WGS84)
    print("SURFACE MANAGEMENT DATA (Reference Dataset):")
    print("-" * 50)
    
    surface_files = [f for f in os.listdir(surface_dir) if f.endswith('.shp')]
    if surface_files:
        sample_surface = os.path.join(surface_dir, surface_files[0])
        try:
            surface_gdf = gpd.read_file(sample_surface)
            print(f"Sample file: {surface_files[0]}")
            print(f"  CRS: {surface_gdf.crs}")
            print(f"  Features: {len(surface_gdf)}")
            
            if not surface_gdf.empty:
                bounds = surface_gdf.bounds
                print(f"  Bounds: X({bounds.minx.min():.6f}, {bounds.maxx.max():.6f}), Y({bounds.miny.min():.6f}, {bounds.maxy.max():.6f})")
                
                # Check intersection with Salton Trough
                intersection = surface_gdf.intersects(salton_bbox)
                intersecting_count = intersection.sum()
                print(f"  Features intersecting Salton Trough: {intersecting_count}/{len(surface_gdf)}")
                
        except Exception as e:
            print(f"Error reading surface data: {e}")
    
    print()
    
    # Check BLM data
    print("BLM INFRASTRUCTURE DATA:")
    print("-" * 50)
    
    blm_files = [f for f in os.listdir(data_dir) if f.endswith('.shp')]
    print(f"Found {len(blm_files)} BLM shapefiles")
    
    intersection_summary = []
    
    for i, filename in enumerate(blm_files[:10]):  # Check first 10 files
        shp_path = os.path.join(data_dir, filename)
        try:
            blm_gdf = gpd.read_file(shp_path)
            name = filename.replace('.shp', '')
            
            print(f"\n{name}:")
            print(f"  CRS: {blm_gdf.crs}")
            print(f"  Features: {len(blm_gdf)}")
            
            if not blm_gdf.empty:
                # Get bounds
                bounds = blm_gdf.bounds
                print(f"  Bounds: X({bounds.minx.min():.6f}, {bounds.maxx.max():.6f}), Y({bounds.miny.min():.6f}, {bounds.maxy.max():.6f})")
                
                # Check if CRS matches Salton Trough
                if blm_gdf.crs != salton_gdf.crs:
                    print(f"  ⚠️  CRS MISMATCH: BLM data is in {blm_gdf.crs}, Salton Trough is in {salton_gdf.crs}")
                    
                    # Try to reproject BLM data to WGS84 for intersection test
                    try:
                        blm_wgs84 = blm_gdf.to_crs('EPSG:4326')
                        print(f"  → Reprojected to WGS84 for intersection test")
                        
                        # Check intersection
                        intersection = blm_wgs84.intersects(salton_bbox)
                        intersecting_count = intersection.sum()
                        print(f"  Features intersecting Salton Trough: {intersecting_count}/{len(blm_wgs84)}")
                        
                        intersection_summary.append({
                            'filename': name,
                            'original_crs': str(blm_gdf.crs),
                            'reprojected_crs': str(blm_wgs84.crs),
                            'total_features': len(blm_wgs84),
                            'intersecting_features': intersecting_count,
                            'intersection_percentage': (intersecting_count / len(blm_wgs84)) * 100
                        })
                        
                    except Exception as e:
                        print(f"  ❌ Failed to reproject: {e}")
                        intersection_summary.append({
                            'filename': name,
                            'original_crs': str(blm_gdf.crs),
                            'reprojected_crs': 'FAILED',
                            'total_features': len(blm_gdf),
                            'intersecting_features': 0,
                            'intersection_percentage': 0
                        })
                else:
                    # Same CRS, check intersection directly
                    intersection = blm_gdf.intersects(salton_bbox)
                    intersecting_count = intersection.sum()
                    print(f"  Features intersecting Salton Trough: {intersecting_count}/{len(blm_gdf)}")
                    
                    intersection_summary.append({
                        'filename': name,
                        'original_crs': str(blm_gdf.crs),
                        'reprojected_crs': 'SAME',
                        'total_features': len(blm_gdf),
                        'intersecting_features': intersecting_count,
                        'intersection_percentage': (intersecting_count / len(blm_gdf)) * 100
                    })
            
        except Exception as e:
            print(f"Error reading {filename}: {e}")
            intersection_summary.append({
                'filename': name,
                'original_crs': 'ERROR',
                'reprojected_crs': 'ERROR',
                'total_features': 0,
                'intersecting_features': 0,
                'intersection_percentage': 0
            })
    
    # Summary
    print("\n" + "="*80)
    print("INTERSECTION SUMMARY")
    print("="*80)
    
    if intersection_summary:
        for item in intersection_summary:
            print(f"{item['filename']}:")
            print(f"  CRS: {item['original_crs']}")
            print(f"  Features: {item['total_features']}")
            print(f"  Intersecting: {item['intersecting_features']} ({item['intersection_percentage']:.1f}%)")
            print()
    
    # Recommendations
    print("RECOMMENDATIONS:")
    print("-" * 50)
    
    crs_issues = [item for item in intersection_summary if item['original_crs'] != 'EPSG:4326']
    intersection_issues = [item for item in intersection_summary if item['intersection_percentage'] == 0]
    
    if crs_issues:
        print(f"1. CRS MISMATCH: {len(crs_issues)} datasets need reprojection from Web Mercator to WGS84")
        print("   - Run the reprojection script: python scripts/reproject_blm_data.py")
        print("   - This will convert coordinates from meters to decimal degrees")
    
    if intersection_issues:
        print(f"2. NO INTERSECTION: {len(intersection_issues)} datasets don't intersect with Salton Trough")
        print("   - These may be national/international datasets that don't cover your study area")
        print("   - Consider if you need these datasets for your analysis")
    
    if not crs_issues and not intersection_issues:
        print("✓ All datasets are properly aligned and intersect with Salton Trough")
    
    print("\n3. After reprojection, update your visualization script to use:")
    print("   'data/BLM_Natl_Rights-of-Way_Planning_Tool_Energy_Designations_Group_Feature_Layer_reprojected'")

def create_intersection_plot(data_dir="data/BLM_Natl_Rights-of-Way_Planning_Tool_Energy_Designations_Group_Feature_Layer"):
    """
    Create a plot showing the spatial relationship between BLM data and Salton Trough.
    """
    
    # Salton Trough bounding box
    SALTON_TROUGH_BBOX = {
        'min_lon': -116.688,
        'min_lat': 32.64,
        'max_lon': -114.472,
        'max_lat': 34.034
    }
    
    salton_bbox = box(SALTON_TROUGH_BBOX['min_lon'], SALTON_TROUGH_BBOX['min_lat'],
                      SALTON_TROUGH_BBOX['max_lon'], SALTON_TROUGH_BBOX['max_lat'])
    salton_gdf = gpd.GeoDataFrame(geometry=[salton_bbox], crs='EPSG:4326')
    
    # Create plot
    fig, ax = plt.subplots(1, 1, figsize=(15, 10))
    
    # Plot Salton Trough study area
    salton_gdf.plot(ax=ax, color='red', alpha=0.3, edgecolor='red', linewidth=2, label='Salton Trough Study Area')
    
    # Plot BLM data (first few files)
    blm_files = [f for f in os.listdir(data_dir) if f.endswith('.shp')][:5]
    
    colors = ['blue', 'green', 'orange', 'purple', 'brown']
    
    for i, filename in enumerate(blm_files):
        shp_path = os.path.join(data_dir, filename)
        try:
            blm_gdf = gpd.read_file(shp_path)
            name = filename.replace('.shp', '')
            
            if not blm_gdf.empty:
                # Reproject if needed
                if blm_gdf.crs != salton_gdf.crs:
                    blm_gdf = blm_gdf.to_crs('EPSG:4326')
                
                # Plot with different color
                blm_gdf.plot(ax=ax, color=colors[i % len(colors)], alpha=0.6, 
                            edgecolor='black', linewidth=0.5, label=name)
                
        except Exception as e:
            print(f"Error plotting {filename}: {e}")
    
    # Customize plot
    ax.set_title('BLM Infrastructure Data vs Salton Trough Study Area\n(Showing spatial relationship and potential CRS issues)', 
                fontsize=14, fontweight='bold')
    ax.set_xlabel('Longitude')
    ax.set_ylabel('Latitude')
    ax.grid(True, alpha=0.3)
    ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=8)
    
    # Set extent to show Salton Trough and surrounding area
    ax.set_xlim(SALTON_TROUGH_BBOX['min_lon'] - 0.5, SALTON_TROUGH_BBOX['max_lon'] + 0.5)
    ax.set_ylim(SALTON_TROUGH_BBOX['min_lat'] - 0.5, SALTON_TROUGH_BBOX['max_lat'] + 0.5)
    
    # Save plot
    output_path = "outputs/shapefile_plots/spatial_intersection_analysis.png"
    Path("outputs/shapefile_plots").mkdir(parents=True, exist_ok=True)
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    
    print(f"\nSpatial intersection plot saved to: {output_path}")

if __name__ == "__main__":
    # Check spatial intersection
    check_spatial_intersection()
    
    # Create visualization plot
    create_intersection_plot()
