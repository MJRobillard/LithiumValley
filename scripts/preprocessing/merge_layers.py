import geopandas as gpd
import pandas as pd
from shapely.geometry import box
from shapely.ops import unary_union
import numpy as np

def main(main_gdf:gpd.GeoDataFrame, merge_gdf:gpd.GeoDataFrame, main_id_col:str, main_admin_col:str, 
                          main_geom_col:str, merge_name_col:str, merge_geom_col:str, preserve_columns:dict=None) -> gpd.GeoDataFrame:
    """
    Implements spatial intersection analysis between BLM areas and federal mergeervations.
    
    This function replicates the SQL logic to create a comprehensive spatial dataset
    that includes intersections, remaining areas, and non-overlapping regions.
    
    Parameters:
    -----------
    main_gdf : GeoDataFrame
        BLM areas data with geometry and administrative information
    merge_gdf : GeoDataFrame
        Federal American Indian mergeervations data with geometry and names
    main_id_col : str
        Column name for BLM ID in main_gdf
    main_admin_col : str
        Column name for administrative agency in main_gdf
    main_geom_col : str
        Column name for geometry in main_gdf
    merge_name_col : str
        Column name for mergeervation name in merge_gdf
    merge_geom_col : str
        Column name for geometry in merge_gdf
    preserve_columns : dict, optional
        Dictionary with 'main' and 'merge' keys containing lists of column names to preserve.
        Example: {'main': ['col1', 'col2'], 'merge': ['col3', 'col4']}
        If None, no additional columns are preserved.
    
    Returns:
    --------
    GeoDataFrame
        Combined spatial dataset with all intersection types and remaining areas
    """
    
    # Ensure geometries are valid and in WGS84 (EPSG:4326)
    main_gdf = main_gdf.copy()
    merge_gdf = merge_gdf.copy()
    
    # Convert to WGS84 if needed
    if main_gdf.crs != 'EPSG:4326':
        main_gdf = main_gdf.to_crs('EPSG:4326')
    if merge_gdf.crs != 'EPSG:4326':
        merge_gdf = merge_gdf.to_crs('EPSG:4326')
    
    # Make geometries valid
    main_gdf[main_geom_col] = main_gdf[main_geom_col].make_valid()
    merge_gdf[merge_geom_col] = merge_gdf[merge_geom_col].make_valid()
    
    # Remove empty geometries
    main_gdf = main_gdf[~main_gdf[main_geom_col].is_empty]
    merge_gdf = merge_gdf[~merge_gdf[merge_geom_col].is_empty]
    
    # Initialize preserve_columns if None
    if preserve_columns is None:
        preserve_columns = {'main': [], 'merge': []}
    elif isinstance(preserve_columns, list):
        # Backward compatibility: convert list to dict format
        preserve_columns = {'main': preserve_columns, 'merge': []}
    
    # Ensure preserve_columns has both keys
    if 'main' not in preserve_columns:
        preserve_columns['main'] = []
    if 'merge' not in preserve_columns:
        preserve_columns['merge'] = []
    
    # Ensure preserve_columns only contains valid columns from respective DataFrames
    preserve_columns['main'] = [col for col in preserve_columns['main'] if col in main_gdf.columns]
    preserve_columns['merge'] = [col for col in preserve_columns['merge'] if col in merge_gdf.columns]
    
    mergeults = []
    
    # 1. Find intersections between BLM areas and mergeervations
    for idx, main_row in main_gdf.iterrows():
        main_geom = main_row[main_geom_col]
        main_id = main_row[main_id_col]
        
        # Skip rows where main_id is None (these are merge-only rows from previous operations)
        if main_id is None:
            continue
            
        main_id = str(main_id)
        main_admin = str(main_row[main_admin_col])
        
        # Get preserved column values from main_gdf
        main_preserved_values = {f"{col}": main_row[col] for col in preserve_columns['main']}
        
        for merge_idx, merge_row in merge_gdf.iterrows():
            merge_geom = merge_row[merge_geom_col]
            merge_name = str(merge_row[merge_name_col])
            
            # Get preserved column values from merge_gdf
            merge_preserved_values = {f"{col}": merge_row[col] for col in preserve_columns['merge']}
            
            if main_geom.intersects(merge_geom):
                intersection = main_geom.intersection(merge_geom)
                if not intersection.is_empty:
                    result_dict = {
                        'main_id': main_id,
                        'main_admin': main_admin,
                        'merge_name': merge_name,
                        'geometry': intersection,
                        'geom_type': 'intersection'
                    }
                    # Add preserved column values from both DataFrames
                    result_dict.update(main_preserved_values)
                    result_dict.update(merge_preserved_values)
                    mergeults.append(result_dict)
    
    # 2. Calculate remaining BLM areas (BLM minus mergeervation overlaps)
    for idx, main_row in main_gdf.iterrows():
        main_geom = main_row[main_geom_col]
        main_id = main_row[main_id_col]
        
        # Skip rows where main_id is None (these are merge-only rows from previous operations)
        if main_id is None:
            continue
            
        main_id = str(main_id)
        main_admin = str(main_row[main_admin_col])
        
        # Get preserved column values from main_gdf
        main_preserved_values = {f"{col}": main_row[col] for col in preserve_columns['main']}
        
        # Find all intersecting mergeervations
        intersecting_merge = []
        for merge_idx, merge_row in merge_gdf.iterrows():
            merge_geom = merge_row[merge_geom_col]
            if main_geom.intersects(merge_geom):
                intersecting_merge.append(merge_geom)
        
        if intersecting_merge:
            # Union all intersecting mergeervations
            merge_union = unary_union(intersecting_merge)
            # Calculate difference
            remaining = main_geom.difference(merge_union)
            
            # Split multipolygons and add each part
            if remaining.geom_type == 'MultiPolygon':
                for geom_part in remaining.geoms:
                    if not geom_part.is_empty:
                        result_dict = {
                            'main_id': main_id,
                            'main_admin': main_admin,
                            'merge_name': None,
                            'geometry': geom_part,
                            'geom_type': 'main_remaining'
                        }
                        # Add preserved column values from main_gdf
                        result_dict.update(main_preserved_values)
                        # Add None values for merge_gdf preserved columns
                        for col in preserve_columns['merge']:
                            result_dict[f"{col}"] = None
                        mergeults.append(result_dict)
            elif not remaining.is_empty:
                result_dict = {
                    'main_id': main_id,
                    'main_admin': main_admin,
                    'merge_name': None,
                    'geometry': remaining,
                    'geom_type': 'main_remaining'
                }
                # Add preserved column values from main_gdf
                result_dict.update(main_preserved_values)
                # Add None values for merge_gdf preserved columns
                for col in preserve_columns['merge']:
                    result_dict[f"{col}"] = None
                mergeults.append(result_dict)
    
    # 3. Calculate remaining mergeervation areas (mergeervations minus BLM overlaps)
    for idx, merge_row in merge_gdf.iterrows():
        merge_geom = merge_row[merge_geom_col]
        merge_name = str(merge_row[merge_name_col])
        
        # Get preserved column values from merge_gdf
        merge_preserved_values = {f"{col}": merge_row[col] for col in preserve_columns['merge']}
        
        # Find all intersecting BLM areas
        intersecting_main = []
        for main_idx, main_row in main_gdf.iterrows():
            main_geom = main_row[main_geom_col]
            if merge_geom.intersects(main_geom):
                intersecting_main.append(main_geom)
        
        if intersecting_main:
            # Union all intersecting BLM areas
            main_union = unary_union(intersecting_main)
            # Calculate difference
            remaining = merge_geom.difference(main_union)
            
            # Split multipolygons and add each part
            if remaining.geom_type == 'MultiPolygon':
                for geom_part in remaining.geoms:
                    if not geom_part.is_empty:
                        result_dict = {
                            'main_id': None,
                            'main_admin': None,
                            'merge_name': merge_name,
                            'geometry': geom_part,
                            'geom_type': 'merge_remaining'
                        }
                        # Add None values for main_gdf preserved columns
                        for col in preserve_columns['main']:
                            result_dict[f"{col}"] = None
                        # Add preserved column values from merge_gdf
                        result_dict.update(merge_preserved_values)
                        mergeults.append(result_dict)
            elif not remaining.is_empty:
                result_dict = {
                    'main_id': None,
                    'main_admin': None,
                    'merge_name': merge_name,
                    'geometry': remaining,
                    'geom_type': 'merge_remaining'
                }
                # Add None values for main_gdf preserved columns
                for col in preserve_columns['main']:
                    result_dict[f"{col}"] = None
                # Add preserved column values from merge_gdf
                result_dict.update(merge_preserved_values)
                mergeults.append(result_dict)
    
    # 4. Find BLM areas with no mergeervation overlap
    for idx, main_row in main_gdf.iterrows():
        main_geom = main_row[main_geom_col]
        main_id = main_row[main_id_col]
        
        # Skip rows where main_id is None (these are merge-only rows from previous operations)
        if main_id is None:
            continue
            
        main_id = str(main_id)
        main_admin = str(main_row[main_admin_col])
        
        # Get preserved column values from main_gdf
        main_preserved_values = {f"{col}": main_row[col] for col in preserve_columns['main']}
        
        # Check if this BLM area intersects with any mergeervation
        has_intersection = False
        for merge_idx, merge_row in merge_gdf.iterrows():
            merge_geom = merge_row[merge_geom_col]
            if main_geom.intersects(merge_geom):
                has_intersection = True
                break
        
        if not has_intersection:
            result_dict = {
                'main_id': main_id,
                'main_admin': main_admin,
                'merge_name': None,
                'geometry': main_geom,
                'geom_type': 'main_no_overlap'
            }
            # Add preserved column values from main_gdf
            result_dict.update(main_preserved_values)
            # Add None values for merge_gdf preserved columns
            for col in preserve_columns['merge']:
                result_dict[f"{col}"] = None
            mergeults.append(result_dict)
    
    # Create final GeoDataFrame
    if mergeults:
        mergeult_gdf = gpd.GeoDataFrame(mergeults, crs='EPSG:4326')
        
        # Add row ID only if it doesn't already exist
        if 'id' not in mergeult_gdf.columns:
            mergeult_gdf.insert(0, 'id', range(1, len(mergeult_gdf) + 1))
        
        # Remove any remaining empty geometries
        mergeult_gdf = mergeult_gdf[~mergeult_gdf['geometry'].is_empty]
        
        return mergeult_gdf
    else:
        # Return empty GeoDataFrame with proper structure
        columns = ['id', 'main_id', 'main_admin', 'merge_name', 'geometry', 'geom_type']
        # Add preserved columns from main_gdf with 'main_' prefix
        for col in preserve_columns['main']:
            columns.append(f"{col}")
        # Add preserved columns from merge_gdf with 'merge_' prefix
        for col in preserve_columns['merge']:
            columns.append(f"{col}")
        return gpd.GeoDataFrame(columns=columns, crs='EPSG:4326')

