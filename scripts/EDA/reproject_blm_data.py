import geopandas as gpd
import os
from pathlib import Path
import shutil

def reproject_blm_data(input_dir="data/BLM_Natl_Rights-of-Way_Planning_Tool_Energy_Designations_Group_Feature_Layer", 
                       output_dir="data/BLM_Natl_Rights-of-Way_Planning_Tool_Energy_Designations_Group_Feature_Layer_reprojected"):
    """
    Reproject BLM data from Web Mercator (EPSG:3857) to WGS84 (EPSG:4326).
    
    Parameters:
    -----------
    input_dir : str
        Path to directory containing original BLM shapefiles
    output_dir : str
        Path to directory for reprojected shapefiles
    """
    
    # Create output directory if it doesn't exist
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    print(f"Reprojecting BLM data from {input_dir} to {output_dir}")
    print("Source CRS: EPSG:3857 (Web Mercator)")
    print("Target CRS: EPSG:4326 (WGS84)")
    
    # Get all shapefile paths
    shapefile_paths = []
    for file in os.listdir(input_dir):
        if file.endswith('.shp'):
            shapefile_paths.append(os.path.join(input_dir, file))
    
    print(f"\nFound {len(shapefile_paths)} shapefiles to reproject")
    
    for i, shp_path in enumerate(shapefile_paths):
        try:
            # Read the shapefile
            gdf = gpd.read_file(shp_path)
            
            # Get the filename without extension
            filename = os.path.basename(shp_path).replace('.shp', '')
            
            print(f"\nProcessing: {filename}")
            print(f"  - Original CRS: {gdf.crs}")
            print(f"  - Features: {len(gdf)}")
            
            # Check if reprojection is needed
            if gdf.crs == 'EPSG:4326':
                print(f"  - Already in WGS84, copying without reprojection")
                # Copy all associated files
                for ext in ['.shp', '.shx', '.dbf', '.prj', '.cpg', '.xml']:
                    src_file = os.path.join(input_dir, f"{filename}{ext}")
                    dst_file = os.path.join(output_dir, f"{filename}{ext}")
                    if os.path.exists(src_file):
                        shutil.copy2(src_file, dst_file)
                continue
            
            # Reproject to WGS84
            gdf_reprojected = gdf.to_crs('EPSG:4326')
            print(f"  - Reprojected CRS: {gdf_reprojected.crs}")
            
            # Save reprojected shapefile
            output_shp_path = os.path.join(output_dir, f"{filename}.shp")
            gdf_reprojected.to_file(output_shp_path)
            
            # Copy other associated files (except .prj which will be updated)
            for ext in ['.shx', '.dbf', '.cpg', '.xml']:
                src_file = os.path.join(input_dir, f"{filename}{ext}")
                dst_file = os.path.join(output_dir, f"{filename}{ext}")
                if os.path.exists(src_file):
                    shutil.copy2(src_file, dst_file)
            
            print(f"  - Saved reprojected shapefile to: {output_shp_path}")
            
        except Exception as e:
            print(f"Error processing {shp_path}: {str(e)}")
    
    print(f"\nReprojection complete! Check {output_dir} for reprojected files.")
    print("You can now use these reprojected files with your visualization script.")

def verify_reprojection(output_dir="data/BLM_Natl_Rights-of-Way_Planning_Tool_Energy_Designations_Group_Feature_Layer_reprojected"):
    """
    Verify that the reprojection was successful by checking CRS and coordinates.
    """
    
    if not os.path.exists(output_dir):
        print(f"Output directory {output_dir} does not exist.")
        return
    
    print(f"\nVerifying reprojection in {output_dir}")
    
    # Get all shapefile paths
    shapefile_paths = []
    for file in os.listdir(output_dir):
        if file.endswith('.shp'):
            shapefile_paths.append(os.path.join(output_dir, file))
    
    for shp_path in shapefile_paths[:3]:  # Check first 3 files
        try:
            gdf = gpd.read_file(shp_path)
            filename = os.path.basename(shp_path).replace('.shp', '')
            
            print(f"\n{filename}:")
            print(f"  - CRS: {gdf.crs}")
            
            if not gdf.empty:
                # Check coordinate ranges
                bounds = gdf.bounds
                min_lon, min_lat = bounds.minx.min(), bounds.miny.min()
                max_lon, max_lat = bounds.maxx.max(), bounds.maxy.max()
                
                print(f"  - Coordinate ranges:")
                print(f"    Longitude: {min_lon:.6f} to {max_lon:.6f}")
                print(f"    Latitude: {min_lat:.6f} to {max_lat:.6f}")
                
                # Check if coordinates are in reasonable WGS84 ranges
                if -180 <= min_lon <= 180 and -90 <= min_lat <= 90:
                    print(f"  - ✓ Coordinates are in valid WGS84 ranges")
                else:
                    print(f"  - ✗ Coordinates are outside valid WGS84 ranges")
            
        except Exception as e:
            print(f"Error verifying {shp_path}: {str(e)}")

if __name__ == "__main__":
    # Reproject the BLM data
    reproject_blm_data()
    
    # Verify the reprojection
    verify_reprojection()
    
    print("\n" + "="*60)
    print("NEXT STEPS:")
    print("1. Update your visualize_shapefiles.py script to use the reprojected data")
    print("2. Change the default data_dir to:")
    print("   'data/BLM_Natl_Rights-of-Way_Planning_Tool_Energy_Designations_Group_Feature_Layer_reprojected'")
    print("3. Run the visualization script again")
    print("="*60)
