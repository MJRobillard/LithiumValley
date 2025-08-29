# Shape File Visualization and Processing Tool

This tool provides comprehensive functionality to process, filter, and visualize shapefiles for the Salton Sea region. It's designed to work with various geospatial datasets and automatically filter them to the specified bounding box coordinates.

## Features

- **Automatic Shapefile Discovery**: Finds all shapefiles in input directories and subdirectories
- **Geographic Filtering**: Clips shapefiles to the Salton Sea region using configurable bounding box coordinates
- **Layer Separation**: Creates separate output files for each input shapefile type
- **Visualization**: Generates overview plots showing all processed layers
- **Analysis Reports**: Creates CSV summaries with processing statistics
- **Coordinate System Handling**: Automatically converts to EPSG:4326 (WGS84) for consistent processing
- **Error Handling**: Robust error handling with detailed logging

## Default Salton Sea Region Bounding Box

The tool uses these default coordinates for the Salton Sea region:
- **Min Longitude**: -116.688
- **Min Latitude**: 32.64
- **Max Longitude**: -114.472
- **Max Latitude**: 34.034

## Usage

### Basic Command

```bash
python scripts/visualize_shapefiles.py \
  --input-dir "data/BLM_Natl_Rights-of-Way_Planning_Tool_Energy_Designations_Group_Feature_Layer" \
  --output-dir "data/processed/unmerged"
```

### Command Line Options

| Option | Description | Default |
|--------|-------------|---------|
| `--input-dir` | Input directory containing shapefiles | Required |
| `--output-dir` | Output directory for processed files | Required |
| `--min-lon` | Minimum longitude for bounding box | -116.688 |
| `--min-lat` | Minimum latitude for bounding box | 32.64 |
| `--max-lon` | Maximum longitude for bounding box | -114.472 |
| `--max-lat` | Maximum latitude for bounding box | 34.034 |
| `--no-plots` | Disable creation of visualization plots | False |
| `--no-report` | Disable creation of summary reports | False |
| `--verbose` | Enable verbose logging | False |

### Examples

#### Process with Custom Bounding Box
```bash
python scripts/visualize_shapefiles.py \
  --input-dir "data/my_shapefiles" \
  --output-dir "data/processed/custom_region" \
  --min-lon -117.0 \
  --min-lat 32.0 \
  --max-lon -114.0 \
  --max-lat 35.0
```

#### Process Without Plots
```bash
python scripts/visualize_shapefiles.py \
  --input-dir "data/my_shapefiles" \
  --output-dir "data/processed/no_plots" \
  --no-plots
```

#### Process Without Reports
```bash
python scripts/visualize_shapefiles.py \
  --input-dir "data/my_shapefiles" \
  --output-dir "data/processed/no_reports" \
  --no-report
```

## Output Files

### Processed Shapefiles
Each input shapefile is processed and saved with the suffix `_SaltonSea.shp`:
- `BLM_DRECP_Development_Focus_Area_(DFA)_SaltonSea.shp`
- `BLM_Solar_Energy_Zone_SaltonSea.shp`
- `Section_368_Corridor_Centerline_SaltonSea.shp`
- etc.

### Visualization Outputs
- **`SaltonSea_Layers_Overview.png`**: Overview plot showing all processed layers
- **`SaltonSea_Layers_Summary.csv`**: Detailed summary report with statistics

## Summary Report Contents

The CSV summary report includes:
- **Layer_Name**: Name of the processed layer
- **Original_Features**: Number of features in the original shapefile
- **Clipped_Features**: Number of features after clipping to the region
- **Reduction_Percent**: Percentage of features removed during clipping
- **Geometry_Type**: Type of geometry (Point, LineString, Polygon, MultiPolygon)
- **Columns_Count**: Number of attribute columns

## Processing Statistics

The tool provides comprehensive statistics:
- Total shapefiles found
- Successfully processed files
- Failed processing attempts
- Total original features
- Total clipped features
- Average reduction percentage

## Supported Geometry Types

The tool handles all common geometry types:
- **Point**: Individual locations (e.g., mileposts, labels)
- **LineString**: Linear features (e.g., corridors, centerlines)
- **Polygon**: Area features (e.g., zones, boundaries)
- **MultiPolygon**: Complex area features

## Error Handling

The tool includes robust error handling:
- **File Reading Errors**: Logs errors and continues with other files
- **Coordinate System Issues**: Automatically converts to WGS84
- **Empty Results**: Handles cases where no features fall within the region
- **Invalid Geometries**: Attempts to fix common geometry issues

## Dependencies

Required Python packages:
- `geopandas`: Geospatial data processing
- `shapely`: Geometric operations
- `matplotlib`: Plotting and visualization
- `pandas`: Data analysis and CSV output
- `numpy`: Numerical operations

Optional packages:
- `contextily`: Basemap support (if available)

## Performance Considerations

- **Spatial Indexing**: Uses spatial indexes for efficient filtering
- **Memory Management**: Processes files one at a time to manage memory usage
- **Progress Logging**: Provides real-time feedback on processing status

## Use Cases

This tool is particularly useful for:
- **Regional Planning**: Filtering national datasets to specific regions
- **Data Preparation**: Preparing shapefiles for analysis or visualization
- **Layer Management**: Organizing multiple shapefile layers for a project
- **Quality Assessment**: Understanding data coverage and feature distribution

## Troubleshooting

### Common Issues

1. **No shapefiles found**: Check that the input directory contains `.shp` files
2. **Empty output**: Verify that the bounding box coordinates are correct
3. **Memory errors**: Process smaller batches of files if memory is limited
4. **Plot generation fails**: Ensure matplotlib is properly installed

### Debug Mode

Use the `--verbose` flag for detailed logging:
```bash
python scripts/visualize_shapefiles.py \
  --input-dir "data/my_shapefiles" \
  --output-dir "data/processed/debug" \
  --verbose
```

## Example Workflow

1. **Prepare Data**: Organize shapefiles in input directory
2. **Run Processing**: Execute the script with appropriate parameters
3. **Review Outputs**: Check processed shapefiles and summary report
4. **Analyze Results**: Use the overview plot to understand layer distribution
5. **Iterate**: Adjust bounding box or processing parameters as needed

## Integration with Other Tools

This tool can be integrated with:
- **GIS Software**: Import processed shapefiles into QGIS, ArcGIS, etc.
- **Analysis Pipelines**: Use processed files as input for further analysis
- **Web Mapping**: Serve processed files through web mapping services
- **Data Validation**: Use summary reports for data quality assessment
