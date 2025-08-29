import geopandas as gpd
import matplotlib.pyplot as plt
import os
from pathlib import Path
import numpy as np
import pandas as pd
from shapely.geometry import box
import csv

def visualize_shapefiles(data_dir="data/BLM_Natl_Rights-of-Way_Planning_Tool_Energy_Designations_Group_Feature_Layer_reprojected", output_dir="outputs/shapefile_plots"):
    """
    Visualize all shapefile layers in the specified directory.
    
    Parameters:
    -----------
    data_dir : str
        Path to directory containing shapefiles (default: "data/processed/surface")
    output_dir : str
        Path to directory containing shapefiles (default: "outputs/shapefile_plots")
    """
    
    # Salton Trough bounding box coordinates
    SALTON_TROUGH_BBOX = {
        'min_lon': -116.688,
        'min_lat': 32.64,
        'max_lon': -114.472,
        'max_lat': 34.034
    }
    
    # Create output directory if it doesn't exist
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    # Initialize summary CSV
    summary_csv_path = os.path.join(output_dir, "shapefile_summary.csv")
    summary_data = []
    
    # Get all shapefile paths
    shapefile_paths = []
    for file in os.listdir(data_dir):
        if file.endswith('.shp'):
            shapefile_paths.append(os.path.join(data_dir, file))
    
    print(f"Found {len(shapefile_paths)} shapefiles to visualize")
    print(f"Filtering to Salton Trough region: {SALTON_TROUGH_BBOX}")
    
    # Create individual plots for each shapefile
    for i, shp_path in enumerate(shapefile_paths):
        try:
            # Read the shapefile
            gdf = gpd.read_file(shp_path)
            
            # Get the filename without extension for the title
            filename = os.path.basename(shp_path).replace('.shp', '')
            
            print(f"Processing: {filename}")
            print(f"  - Original features: {len(gdf)}")
            print(f"  - Geometry type: {gdf.geometry.geom_type.iloc[0] if not gdf.empty else 'Empty'}")
            print(f"  - CRS: {gdf.crs}")
            
            # Clip the shapefile to Salton Trough region before processing
            gdf_clipped = clip_to_salton_trough(gdf, SALTON_TROUGH_BBOX)
            print(f"  - Features after clipping: {len(gdf_clipped)}")
            
            if gdf_clipped.empty:
                print(f"  - Skipping {filename} - no features in Salton Trough region after clipping")
                continue
            
            # Create the plot with clipped data
            fig, ax = plt.subplots(1, 1, figsize=(12, 10))
            
            # Try to find a good attribute column for coloring
            color_column = find_best_color_column(gdf_clipped)
            
            if color_column and len(gdf_clipped[color_column].unique()) > 1:
                # Use distinct colors based on attribute values
                plot_with_attribute_colors(gdf_clipped, ax, color_column, filename)
            else:
                # Use single color if no good attribute found
                gdf_clipped.plot(ax=ax, color='lightblue', alpha=0.7, edgecolor='black', linewidth=0.5)
                ax.set_title(f'{filename}\n({len(gdf_clipped)} features in Salton Trough)', fontsize=14, fontweight='bold')
            
            # Add labels and grid
            ax.set_xlabel('Longitude')
            ax.set_ylabel('Latitude')
            ax.grid(True, alpha=0.3)
            ax.set_xticks([])
            ax.set_yticks([])
            
            # Set the plot extent to Salton Trough region
            ax.set_xlim(SALTON_TROUGH_BBOX['min_lon'], SALTON_TROUGH_BBOX['max_lon'])
            ax.set_ylim(SALTON_TROUGH_BBOX['min_lat'], SALTON_TROUGH_BBOX['max_lat'])
            
            # Save the plot
            output_path = os.path.join(output_dir, f"{filename}.png")
            plt.savefig(output_path, dpi=300, bbox_inches='tight')
            plt.close()
            
            print(f"  - Saved plot to: {output_path}")
            
            # Update summary CSV with clipped shapefile's attributes
            update_summary_csv(gdf_clipped, filename, summary_csv_path, summary_data)
            
        except Exception as e:
            print(f"Error processing {shp_path}: {str(e)}")
            # Still try to add to summary even if plotting failed
            try:
                gdf = gpd.read_file(shp_path)
                filename = os.path.basename(shp_path).replace('.shp', '')
                update_summary_csv(gdf, filename, summary_csv_path, summary_data)
            except:
                pass
    
    # Create a combined overview plot
    print("\nCreating combined overview plot...")
    create_combined_plot(shapefile_paths, output_dir, SALTON_TROUGH_BBOX)
    
    # Print summary of CSV creation
    print(f"\nSummary CSV created/updated: {summary_csv_path}")
    print(f"Total shapefiles processed: {len(summary_data)}")
    print("Check the CSV file for detailed attribute information about each shapefile.")

def find_best_color_column(gdf):
    """
    Find the best column to use for coloring based on data characteristics.
    Prioritizes columns with categorical data that would make good color schemes.
    """
    if gdf.empty:
        return None
    
    # Look for columns that might contain names, types, or categories
    priority_keywords = ['name', 'type', 'agency', 'category', 'class', 'status', 'owner', 'mgt']
    
    for col in gdf.columns:
        if col.lower() in ['geometry']:
            continue
            
        # Check if column has reasonable number of unique values for coloring
        unique_count = gdf[col].nunique()
        
        if unique_count > 1 and unique_count <= 20:  # Good range for distinct colors
            # Check if it contains text data
            if gdf[col].dtype == 'object' or gdf[col].dtype == 'string':
                return col
            # Also consider numeric columns with few unique values
            elif unique_count <= 10:
                return col
    
    # If no good column found, return the first non-geometry column with reasonable unique values
    for col in gdf.columns:
        if col.lower() not in ['geometry'] and gdf[col].nunique() <= 15:
            return col
    
    return None

def plot_with_attribute_colors(gdf, ax, color_column, filename):
    """
    Plot the shapefile with distinct colors based on attribute values.
    """
    # Get unique values and create color map
    unique_values = gdf[color_column].unique()
    unique_values = [val for val in unique_values if pd.notna(val)]  # Remove NaN values
    
    if len(unique_values) <= 1:
        # Fall back to single color
        gdf.plot(ax=ax, color='lightblue', alpha=0.7, edgecolor='black', linewidth=0.5)
        ax.set_title(f'{filename}\n({len(gdf)} features)', fontsize=14, fontweight='bold')
        return
    
    # Create distinct colors
    if len(unique_values) <= 10:
        colors = plt.cm.Set3(np.linspace(0, 1, len(unique_values)))
    else:
        colors = plt.cm.tab20(np.linspace(0, 1, len(unique_values)))
    
    # Plot each unique value with a different color
    for i, value in enumerate(unique_values):
        subset = gdf[gdf[color_column] == value]
        if not subset.empty:
            subset.plot(ax=ax, color=colors[i], alpha=0.7, edgecolor='black', 
                       linewidth=0.5, label=f"{color_column}: {value}")
    
    # Add title with attribute information
    ax.set_title(f'{filename}\n({len(gdf)} features, colored by {color_column})', 
                fontsize=14, fontweight='bold')
    
    # Add legend if there are multiple values
    if len(unique_values) > 1:
        ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=8)

def create_combined_plot(shapefile_paths, output_dir, bbox):
    """Create a combined plot showing all shapefiles together with distinct colors, filtered to Salton Trough."""
    
    fig, ax = plt.subplots(1, 1, figsize=(16, 12))
    
    # Track used colors to avoid conflicts
    used_colors = set()
    
    # Plot each shapefile with different colors
    for i, shp_path in enumerate(shapefile_paths):
        try:
            gdf = gpd.read_file(shp_path)
            filename = os.path.basename(shp_path).replace('.shp', '')
            
            # Clip to Salton Trough region
            gdf_clipped = clip_to_salton_trough(gdf, bbox)
            
            if gdf_clipped.empty:
                print(f"    No features in Salton Trough region for {filename}")
                continue
            else:
                print(f"    Clipped to Salton Trough for {filename}")
            
            # Find color column for this shapefile
            color_column = find_best_color_column(gdf_clipped)
            
            if color_column and gdf_clipped[color_column].nunique() > 1:
                # Use attribute-based coloring
                plot_combined_with_attributes(gdf_clipped, ax, color_column, filename, used_colors)
            else:
                # Use single color for the entire layer
                color = get_distinct_color(used_colors)
                gdf_clipped.plot(ax=ax, color=color, alpha=0.6, edgecolor='black', 
                        linewidth=0.3, label=filename)
                used_colors.add(color)
            
        except Exception as e:
            print(f"Error in combined plot for {shp_path}: {str(e)}")
    
    # Customize the combined plot
    ax.set_title('Salton Trough Region - All Layers Combined\n(May include nearby areas for complete coverage)', 
                fontsize=16, fontweight='bold')
    ax.set_xlabel('Longitude')
    ax.set_ylabel('Latitude')
    ax.grid(True, alpha=0.3)
    
    # Set the plot extent to Salton Trough region
    ax.set_xlim(bbox['min_lon'], bbox['max_lon'])
    ax.set_ylim(bbox['min_lat'], bbox['max_lat'])
    
    # Add legend (outside the plot to avoid overlap)
    ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=8)
    
    # Save combined plot
    combined_path = os.path.join(output_dir, "combined_all_layers.png")
    plt.savefig(combined_path, dpi=300, bbox_inches='tight')
    plt.close()
    
    print(f"Combined plot saved to: {combined_path}")

def update_summary_csv(gdf, filename, csv_path, summary_data):
    """
    Update the summary CSV with information about a shapefile.
    Creates the CSV if it doesn't exist, or updates it if it does.
    
    Parameters:
    -----------
    gdf : GeoDataFrame
        The GeoDataFrame to summarize
    filename : str
        Name of the shapefile
    csv_path : str
        Path to the CSV file
    summary_data : list
        List to store summary data
    """
    try:
        # Extract basic information
        summary_row = {
            'filename': filename,
            'total_features': len(gdf),
            'geometry_type': gdf.geometry.geom_type.iloc[0] if not gdf.empty else 'Empty',
            'crs': str(gdf.crs) if gdf.crs else 'Unknown',
            'columns': ', '.join([col for col in gdf.columns if col != 'geometry']),
            'bbox_minx': gdf.bounds.minx.min() if not gdf.empty else None,
            'bbox_miny': gdf.bounds.miny.min() if not gdf.empty else None,
            'bbox_maxx': gdf.bounds.maxx.max() if not gdf.empty else None,
            'bbox_maxy': gdf.bounds.maxy.max() if not gdf.empty else None,
            'processing_timestamp': pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        # Add attribute information
        for col in gdf.columns:
            if col != 'geometry':
                col_name = col.replace(' ', '_').replace('-', '_')
                if gdf[col].dtype == 'object' or gdf[col].dtype == 'string':
                    # For text columns, show unique values count and sample values
                    unique_vals = gdf[col].nunique()
                    sample_vals = gdf[col].dropna().head(3).tolist()
                    summary_row[f'{col_name}_unique_count'] = unique_vals
                    summary_row[f'{col_name}_sample_values'] = '; '.join([str(v) for v in sample_vals])
                else:
                    # For numeric columns, show min, max, mean
                    try:
                        summary_row[f'{col_name}_min'] = gdf[col].min()
                        summary_row[f'{col_name}_max'] = gdf[col].max()
                        summary_row[f'{col_name}_mean'] = gdf[col].mean()
                    except:
                        summary_row[f'{col_name}_min'] = None
                        summary_row[f'{col_name}_max'] = None
                        summary_row[f'{col_name}_mean'] = None
        
        # Add to summary data
        summary_data.append(summary_row)
        
        # Write/update CSV
        if os.path.exists(csv_path):
            # Read existing CSV and update
            existing_df = pd.read_csv(csv_path)
            # Remove existing row for this filename if it exists
            existing_df = existing_df[existing_df['filename'] != filename]
            # Add new row
            updated_df = pd.concat([existing_df, pd.DataFrame([summary_row])], ignore_index=True)
            updated_df.to_csv(csv_path, index=False)
            print(f"  - Updated summary CSV: {csv_path}")
        else:
            # Create new CSV
            df = pd.DataFrame([summary_row])
            df.to_csv(csv_path, index=False)
            print(f"  - Created summary CSV: {csv_path}")
            
    except Exception as e:
        print(f"  - Warning: Could not update summary CSV: {str(e)}")

def clip_to_salton_trough(gdf, bbox):
    """
    Clip a GeoDataFrame to the Salton Trough bounding box, trimming geometries to fit within the bounds.
    
    Parameters:
    -----------
    gdf : GeoDataFrame
        Input GeoDataFrame to clip
    bbox : dict
        Dictionary with min_lon, min_lat, max_lon, max_lat keys
        
    Returns:
    --------
    GeoDataFrame
        Clipped GeoDataFrame with geometries trimmed to the bounding box
    """
    if gdf.empty:
        return gdf
    
    try:
        # Create a bounding box polygon
        bbox_polygon = box(bbox['min_lon'], bbox['min_lat'], 
                           bbox['max_lon'], bbox['max_lat'])
        
        # Create a GeoDataFrame with the bounding box
        bbox_gdf = gpd.GeoDataFrame(geometry=[bbox_polygon], crs=gdf.crs)
        
        # Clip the geometries to the bounding box
        clipped_gdf = gpd.clip(gdf, bbox_gdf)
        
        if clipped_gdf.empty:
            print(f"    Warning: Clipping resulted in empty dataset")
            return gdf  # Return original if clipping fails
            
        return clipped_gdf
        
    except Exception as e:
        print(f"    Warning: Could not clip to bounding box: {e}")
        # Fallback: try filtering instead
        try:
            # Filter features that intersect with the bounding box
            bbox_polygon = box(bbox['min_lon'], bbox['min_lat'], 
                               bbox['max_lon'], bbox['max_lat'])
            bbox_gdf = gpd.GeoDataFrame(geometry=[bbox_polygon], crs=gdf.crs)
            
            # Find intersecting features
            intersecting_gdf = gdf[gdf.intersects(bbox_polygon)]
            
            if intersecting_gdf.empty:
                return gdf  # Return original if no intersection
                
            return intersecting_gdf
            
        except Exception as e2:
            print(f"    Warning: Fallback filtering also failed: {e2}")
            # Return original data as last resort
            return gdf

def filter_to_salton_trough(gdf, bbox):
    """
    Filter a GeoDataFrame to only include features within the Salton Trough bounding box.
    This is a fallback method when clipping fails.
    
    Parameters:
    -----------
    gdf : GeoDataFrame
        Input GeoDataFrame to filter
    bbox : dict
        Dictionary with min_lon, min_lat, max_lon, max_lat keys
        
    Returns:
    --------
    GeoDataFrame
        Filtered GeoDataFrame containing only features in the bounding box
    """
    if gdf.empty:
        return gdf
    
    try:
        # Create a bounding box polygon
        bbox_polygon = box(bbox['min_lon'], bbox['min_lat'], 
                           bbox['max_lon'], bbox['max_lat'])
        
        # Create a GeoDataFrame with the bounding box
        bbox_gdf = gpd.GeoDataFrame(geometry=[bbox_polygon], crs=gdf.crs)
        
        # Find intersecting features
        intersecting_gdf = gdf[gdf.intersects(bbox_polygon)]
        
        if intersecting_gdf.empty:
            return gdf  # Return original if no intersection
            
        return intersecting_gdf
        
    except Exception as e:
        print(f"    Warning: Could not filter by bounding box: {e}")
        # Fallback: return original data
        return gdf

def plot_combined_with_attributes(gdf, ax, color_column, filename, used_colors):
    """Plot a shapefile in the combined plot using attribute-based colors."""
    unique_values = gdf[color_column].unique()
    unique_values = [val for val in unique_values if pd.notna(val)]
    
    for value in unique_values:
        subset = gdf[gdf[color_column] == value]
        if not subset.empty:
            color = get_distinct_color(used_colors)
            subset.plot(ax=ax, color=color, alpha=0.6, edgecolor='black', 
                       linewidth=0.3, label=f"{filename}: {value}")
            used_colors.add(color)

def get_distinct_color(used_colors):
    """Get a distinct color that hasn't been used yet."""
    all_colors = plt.cm.Set3(np.linspace(0, 1, 12))
    for color in all_colors:
        if color not in used_colors:
            return color
    # If all colors used, return a random one
    return np.random.rand(3,)

def analyze_shapefile_attributes(data_dir="data/BLM_Natl_Rights-of-Way_Planning_Tool_Energy_Designations_Group_Feature_Layer_reprojected"):
    """Analyze and print basic information about each shapefile's attributes."""
    
    print("\n" + "="*60)
    print("SHAPEFILE ATTRIBUTE ANALYSIS")
    print("="*60)
    
    for file in os.listdir(data_dir):
        if file.endswith('.shp'):
            shp_path = os.path.join(data_dir, file)
            try:
                gdf = gpd.read_file(shp_path)
                filename = os.path.basename(shp_path).replace('.shp', '')
                
                print(f"\n{filename}:")
                print(f"  Columns: {list(gdf.columns)}")
                print(f"  Data types: {gdf.dtypes.to_dict()}")
                
                # Show sample data for first few rows
                if not gdf.empty:
                    print(f"  Sample data (first 3 rows):")
                    print(gdf.head(3).to_string())
                    
                    # Show unique value counts for potential color columns
                    color_column = find_best_color_column(gdf)
                    if color_column:
                        unique_vals = gdf[color_column].nunique()
                        print(f"  Recommended color column: {color_column} ({unique_vals} unique values)")
                
            except Exception as e:
                print(f"Error analyzing {shp_path}: {str(e)}")

if __name__ == "__main__":
    # Run the visualization
    visualize_shapefiles()
    
    # Run the attribute analysis
    analyze_shapefile_attributes()
    
    print("\nVisualization complete! Check the output directory for all plots.")
