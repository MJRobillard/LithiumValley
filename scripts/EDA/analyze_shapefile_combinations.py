"""
Shapefile Combination Analysis Tool

This script analyzes shapefiles in the unmerged directory to determine the best approach
for combining them: joins vs unions. It examines geometry types, attribute compatibility,
spatial relationships, and provides recommendations for optimal combination strategies.

Usage:
    python scripts/analyze_shapefile_combinations.py --input-dir "data/processed/unmerged"
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Set
import geopandas as gpd
import pandas as pd
import numpy as np
from shapely.geometry import box
from shapely.ops import unary_union
import matplotlib.pyplot as plt
import seaborn as sns

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def analyze_shapefile(shapefile_path: Path) -> Dict:
    """Analyze a single shapefile and extract key information."""
    try:
        gdf = gpd.read_file(str(shapefile_path))
        
        # Basic info
        info = {
            'path': shapefile_path,
            'name': shapefile_path.stem,
            'feature_count': len(gdf),
            'geometry_type': gdf.geometry.geom_type.iloc[0] if not gdf.empty else None,
            'crs': str(gdf.crs),
            'bbox': gdf.total_bounds.tolist() if not gdf.empty else None,
            'columns': list(gdf.columns),
            'column_types': {col: str(gdf[col].dtype) for col in gdf.columns if col != 'geometry'},
            'has_attributes': len(gdf.columns) > 1,
            'attribute_count': len(gdf.columns) - 1,  # Exclude geometry column
            'area_km2': None,
            'length_km': None,
            'spatial_density': None
        }
        
        if not gdf.empty:
            # Calculate spatial metrics
            if info['geometry_type'] in ['Polygon', 'MultiPolygon']:
                # Convert to projected CRS for area calculation
                try:
                    gdf_proj = gdf.to_crs('EPSG:3857')  # Web Mercator
                    areas = gdf_proj.geometry.area / 1e6  # Convert to km²
                    info['area_km2'] = areas.sum()
                    info['spatial_density'] = info['feature_count'] / info['area_km2'] if info['area_km2'] > 0 else 0
                except Exception as e:
                    logger.warning(f"Could not calculate area for {shapefile_path.name}: {e}")
                    
            elif info['geometry_type'] in ['LineString', 'MultiLineString']:
                try:
                    gdf_proj = gdf.to_crs('EPSG:3857')
                    lengths = gdf_proj.geometry.length / 1000  # Convert to km
                    info['length_km'] = lengths.sum()
                    info['spatial_density'] = info['feature_count'] / info['length_km'] if info['length_km'] > 0 else 0
                except Exception as e:
                    logger.warning(f"Could not calculate length for {shapefile_path.name}: {e}")
                    
            elif info['geometry_type'] == 'Point':
                # For points, calculate density based on bounding box area
                try:
                    bbox = box(*gdf.total_bounds)
                    bbox_proj = gpd.GeoDataFrame(geometry=[bbox], crs=gdf.crs).to_crs('EPSG:3857')
                    bbox_area = bbox_proj.geometry.area.iloc[0] / 1e6  # km²
                    info['spatial_density'] = info['feature_count'] / bbox_area if bbox_area > 0 else 0
                except Exception as e:
                    logger.warning(f"Could not calculate density for {shapefile_path.name}: {e}")
        
        return info
        
    except Exception as e:
        logger.error(f"Error analyzing {shapefile_path}: {e}")
        return {
            'path': shapefile_path,
            'name': shapefile_path.stem,
            'error': str(e)
        }


def find_shapefiles(input_dir: Path) -> List[Path]:
    """Find all shapefiles in the input directory."""
    shapefiles = []
    for shp_file in input_dir.glob("*.shp"):
        shapefiles.append(shp_file)
    return sorted(shapefiles)


def analyze_combinations(shapefile_infos: List[Dict]) -> Dict:
    """Analyze potential combinations between shapefiles."""
    combinations = []
    
    for i, info1 in enumerate(shapefile_infos):
        if 'error' in info1:
            continue
            
        for j, info2 in enumerate(shapefile_infos[i+1:], i+1):
            if 'error' in info2:
                continue
                
            # Analyze compatibility
            compatibility = analyze_compatibility(info1, info2)
            combinations.append(compatibility)
    
    return combinations


def analyze_compatibility(info1: Dict, info2: Dict) -> Dict:
    """Analyze compatibility between two shapefiles."""
    
    # Geometry type compatibility
    geom_compatible = info1['geometry_type'] == info2['geometry_type']
    
    # Attribute compatibility
    common_columns = set(info1['columns']) & set(info2['columns'])
    common_columns.discard('geometry')
    attribute_compatible = len(common_columns) > 0
    
    # Spatial overlap analysis
    spatial_relationship = analyze_spatial_relationship(info1, info2)
    
    # Determine recommended approach
    recommended_approach = determine_recommended_approach(
        geom_compatible, attribute_compatible, spatial_relationship, info1, info2, common_columns
    )
    
    # Calculate combination score
    combination_score = calculate_combination_score(
        geom_compatible, attribute_compatible, spatial_relationship, info1, info2
    )
    
    return {
        'file1': info1['name'],
        'file2': info2['name'],
        'geometry_compatible': geom_compatible,
        'attribute_compatible': attribute_compatible,
        'common_columns': list(common_columns),
        'spatial_relationship': spatial_relationship,
        'recommended_approach': recommended_approach,
        'combination_score': combination_score,
        'file1_geometry': info1['geometry_type'],
        'file2_geometry': info2['geometry_type'],
        'file1_features': info1['feature_count'],
        'file2_features': info2['feature_count'],
        'file1_attributes': info1['attribute_count'],
        'file2_attributes': info2['attribute_count']
    }


def analyze_spatial_relationship(info1: Dict, info2: Dict) -> str:
    """Analyze spatial relationship between two shapefiles."""
    if not info1['bbox'] or not info2['bbox']:
        return 'unknown'
    
    # Create bounding boxes
    bbox1 = box(*info1['bbox'])
    bbox2 = box(*info2['bbox'])
    
    # Check intersection
    if bbox1.intersects(bbox2):
        intersection = bbox1.intersection(bbox2)
        intersection_area = intersection.area
        bbox1_area = bbox1.area
        bbox2_area = bbox2.area
        
        # Calculate overlap percentages
        overlap1 = intersection_area / bbox1_area if bbox1_area > 0 else 0
        overlap2 = intersection_area / bbox2_area if bbox2_area > 0 else 0
        
        if overlap1 > 0.8 and overlap2 > 0.8:
            return 'high_overlap'
        elif overlap1 > 0.5 or overlap2 > 0.5:
            return 'moderate_overlap'
        elif overlap1 > 0.1 or overlap2 > 0.1:
            return 'low_overlap'
        else:
            return 'minimal_overlap'
    else:
        return 'no_overlap'


def determine_recommended_approach(geom_compatible: bool, attribute_compatible: bool, 
                                 spatial_relationship: str, info1: Dict, info2: Dict, common_columns: Set[str]) -> str:
    """Determine the recommended approach for combining two shapefiles."""
    
    if not geom_compatible:
        return 'separate_layers'  # Can't combine different geometry types
    
    # High overlap scenarios
    if spatial_relationship in ['high_overlap', 'moderate_overlap']:
        if attribute_compatible and len(common_columns) > 2:
            return 'union_with_join'
        else:
            return 'union'
    
    # Low overlap scenarios
    elif spatial_relationship in ['low_overlap', 'minimal_overlap']:
        if attribute_compatible:
            return 'join'
        else:
            return 'union'
    
    # No overlap
    elif spatial_relationship == 'no_overlap':
        if attribute_compatible:
            return 'join'
        else:
            return 'union'
    
    return 'separate_layers'


def calculate_combination_score(geom_compatible: bool, attribute_compatible: bool,
                              spatial_relationship: str, info1: Dict, info2: Dict) -> float:
    """Calculate a score indicating how well two shapefiles can be combined."""
    score = 0.0
    
    # Geometry compatibility (highest weight)
    if geom_compatible:
        score += 40
    else:
        return 0  # Can't combine different geometry types
    
    # Attribute compatibility
    if attribute_compatible:
        score += 25
        # Bonus for more common columns
        common_cols = set(info1['columns']) & set(info2['columns'])
        common_cols.discard('geometry')
        score += min(len(common_cols) * 2, 15)
    
    # Spatial relationship
    spatial_scores = {
        'high_overlap': 20,
        'moderate_overlap': 15,
        'low_overlap': 10,
        'minimal_overlap': 5,
        'no_overlap': 0,
        'unknown': 0
    }
    score += spatial_scores.get(spatial_relationship, 0)
    
    # Feature count balance (prefer similar sizes)
    feature_ratio = min(info1['feature_count'], info2['feature_count']) / max(info1['feature_count'], info2['feature_count'])
    score += feature_ratio * 10
    
    return min(score, 100)


def create_combination_matrix(combinations: List[Dict]) -> pd.DataFrame:
    """Create a matrix showing combination scores between all shapefiles."""
    if not combinations:
        return pd.DataFrame()
    
    # Get unique file names
    files = sorted(list(set([c['file1'] for c in combinations] + [c['file2'] for c in combinations])))
    
    # Create matrix
    matrix_data = []
    for file1 in files:
        row = {'File': file1}
        for file2 in files:
            if file1 == file2:
                row[file2] = 100  # Self-combination
            else:
                # Find combination
                combo = next((c for c in combinations 
                            if (c['file1'] == file1 and c['file2'] == file2) or
                               (c['file1'] == file2 and c['file2'] == file1)), None)
                row[file2] = combo['combination_score'] if combo else 0
        matrix_data.append(row)
    
    return pd.DataFrame(matrix_data).set_index('File')


def create_recommendations_report(combinations: List[Dict], output_dir: Path) -> None:
    """Create a detailed recommendations report."""
    if not combinations:
        logger.warning("No combinations to analyze")
        return
    
    # Convert to DataFrame
    df = pd.DataFrame(combinations)
    
    # Sort by combination score
    df_sorted = df.sort_values('combination_score', ascending=False)
    
    # Save detailed report
    csv_path = output_dir / "Shapefile_Combination_Analysis.csv"
    df_sorted.to_csv(csv_path, index=False)
    logger.info(f"Combination analysis saved: {csv_path}")
    
    # Create summary report
    summary_path = output_dir / "Combination_Recommendations_Summary.txt"
    with open(summary_path, 'w') as f:
        f.write("SHAPEFILE COMBINATION RECOMMENDATIONS\n")
        f.write("=" * 50 + "\n\n")
        
        # High priority combinations
        high_priority = df_sorted[df_sorted['combination_score'] >= 80]
        if not high_priority.empty:
            f.write("HIGH PRIORITY COMBINATIONS (Score >= 80):\n")
            f.write("-" * 40 + "\n")
            for _, row in high_priority.iterrows():
                f.write(f"• {row['file1']} + {row['file2']}\n")
                f.write(f"  Score: {row['combination_score']:.1f}\n")
                f.write(f"  Approach: {row['recommended_approach']}\n")
                f.write(f"  Reason: {get_recommendation_reason(row)}\n\n")
        
        # Medium priority combinations
        medium_priority = df_sorted[(df_sorted['combination_score'] >= 60) & (df_sorted['combination_score'] < 80)]
        if not medium_priority.empty:
            f.write("MEDIUM PRIORITY COMBINATIONS (Score 60-79):\n")
            f.write("-" * 40 + "\n")
            for _, row in medium_priority.iterrows():
                f.write(f"• {row['file1']} + {row['file2']}\n")
                f.write(f"  Score: {row['combination_score']:.1f}\n")
                f.write(f"  Approach: {row['recommended_approach']}\n\n")
        
        # Summary statistics
        f.write("SUMMARY STATISTICS:\n")
        f.write("-" * 20 + "\n")
        f.write(f"Total combinations analyzed: {len(combinations)}\n")
        f.write(f"High priority combinations: {len(high_priority)}\n")
        f.write(f"Medium priority combinations: {len(medium_priority)}\n")
        f.write(f"Average combination score: {df_sorted['combination_score'].mean():.1f}\n")
        
        # Approach distribution
        approach_counts = df_sorted['recommended_approach'].value_counts()
        f.write(f"\nRecommended approaches:\n")
        for approach, count in approach_counts.items():
            f.write(f"  {approach}: {count}\n")
    
    logger.info(f"Recommendations summary saved: {summary_path}")


def get_recommendation_reason(row: pd.Series) -> str:
    """Get a human-readable reason for the recommendation."""
    reasons = []
    
    if row['geometry_compatible']:
        reasons.append("Same geometry type")
    else:
        reasons.append("Different geometry types")
    
    if row['attribute_compatible']:
        reasons.append(f"{len(row['common_columns'])} common attributes")
    
    if row['spatial_relationship'] in ['high_overlap', 'moderate_overlap']:
        reasons.append(f"{row['spatial_relationship'].replace('_', ' ')}")
    
    return "; ".join(reasons)


def create_visualization(combinations: List[Dict], output_dir: Path) -> None:
    """Create visualizations of the combination analysis."""
    if not combinations:
        return
    
    df = pd.DataFrame(combinations)
    
    # Create combination matrix heatmap
    matrix = create_combination_matrix(combinations)
    
    plt.figure(figsize=(12, 10))
    sns.heatmap(matrix, annot=True, cmap='RdYlGn', center=50, 
                cbar_kws={'label': 'Combination Score'})
    plt.title('Shapefile Combination Compatibility Matrix', fontsize=16, fontweight='bold')
    plt.xlabel('Shapefile 2', fontsize=12)
    plt.ylabel('Shapefile 1', fontsize=12)
    plt.xticks(rotation=45, ha='right')
    plt.yticks(rotation=0)
    plt.tight_layout()
    
    # Save heatmap
    heatmap_path = output_dir / "Combination_Compatibility_Matrix.png"
    plt.savefig(heatmap_path, dpi=300, bbox_inches='tight')
    logger.info(f"Compatibility matrix saved: {heatmap_path}")
    plt.close()
    
    # Create approach distribution chart
    plt.figure(figsize=(10, 6))
    approach_counts = df['recommended_approach'].value_counts()
    colors = plt.cm.Set3(np.linspace(0, 1, len(approach_counts)))
    
    bars = plt.bar(approach_counts.index, approach_counts.values, color=colors)
    plt.title('Distribution of Recommended Combination Approaches', fontsize=14, fontweight='bold')
    plt.xlabel('Approach', fontsize=12)
    plt.ylabel('Number of Combinations', fontsize=12)
    plt.xticks(rotation=45, ha='right')
    
    # Add value labels on bars
    for bar, count in zip(bars, approach_counts.values):
        plt.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.5, 
                str(count), ha='center', va='bottom')
    
    plt.tight_layout()
    
    # Save approach chart
    approach_path = output_dir / "Combination_Approaches_Distribution.png"
    plt.savefig(approach_path, dpi=300, bbox_inches='tight')
    logger.info(f"Approach distribution chart saved: {approach_path}")
    plt.close()


def main():
    """Main function."""
    parser = argparse.ArgumentParser(
        description="Analyze shapefile combinations for optimal joining/union strategies"
    )
    parser.add_argument(
        "--input-dir", 
        type=str, 
        required=True,
        help="Input directory containing shapefiles to analyze"
    )
    parser.add_argument(
        "--output-dir", 
        type=str, 
        default="data/processed/analysis",
        help="Output directory for analysis results"
    )
    parser.add_argument(
        "--verbose", 
        action="store_true",
        help="Enable verbose logging"
    )
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    input_path = Path(args.input_dir)
    output_path = Path(args.output_dir)
    
    # Ensure output directory exists
    output_path.mkdir(parents=True, exist_ok=True)
    
    logger.info(f"Analyzing shapefiles in: {input_path}")
    logger.info(f"Output directory: {output_path}")
    
    # Find and analyze shapefiles
    shapefiles = find_shapefiles(input_path)
    logger.info(f"Found {len(shapefiles)} shapefiles to analyze")
    
    if not shapefiles:
        logger.error("No shapefiles found")
        return 1
    
    # Analyze individual shapefiles
    logger.info("Analyzing individual shapefiles...")
    shapefile_infos = []
    for shapefile in shapefiles:
        info = analyze_shapefile(shapefile)
        shapefile_infos.append(info)
        logger.info(f"  {shapefile.name}: {info.get('geometry_type', 'unknown')} - {info.get('feature_count', 0)} features")
    
    # Analyze combinations
    logger.info("Analyzing shapefile combinations...")
    combinations = analyze_combinations(shapefile_infos)
    logger.info(f"Analyzed {len(combinations)} combinations")
    
    # Create outputs
    if combinations:
        create_recommendations_report(combinations, output_path)
        create_visualization(combinations, output_path)
        
        # Print top recommendations
        df = pd.DataFrame(combinations)
        top_combinations = df.nlargest(5, 'combination_score')
        
        print("\n" + "="*60)
        print("TOP 5 RECOMMENDED COMBINATIONS")
        print("="*60)
        for _, row in top_combinations.iterrows():
            print(f"• {row['file1']} + {row['file2']}")
            print(f"  Score: {row['combination_score']:.1f}")
            print(f"  Approach: {row['recommended_approach']}")
            print(f"  Geometry: {row['file1_geometry']} + {row['file2_geometry']}")
            print(f"  Features: {row['file1_features']} + {row['file2_features']}")
            print()
    
    logger.info("Analysis complete!")
    return 0


if __name__ == "__main__":
    exit(main())
