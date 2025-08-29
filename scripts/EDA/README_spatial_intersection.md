# Spatial Intersection Processor

A parameterized Python script that performs spatial intersections between two tables and creates a new table with overlapping geometries on top and remaining larger areas below.

## Features

- **Fully Parameterized**: All table names, column names, and spatial reference systems are configurable
- **Flexible Input**: Works with any two spatial tables regardless of their structure
- **Proper Layering**: Creates a result table where smaller intersection geometries appear on top of larger remaining areas
- **Database Agnostic**: Works with any PostgreSQL/PostGIS database
- **Error Handling**: Comprehensive error handling and logging
- **Context Manager**: Safe database connection management

## How It Works

The script performs the following spatial operations:

1. **Intersections**: Finds overlapping areas between the two input tables
2. **Table1 Remaining**: Areas from the first table minus overlaps with the second table
3. **Table2 Remaining**: Areas from the second table minus overlaps with the first table  
4. **Table1 No Overlap**: Areas from the first table with no overlap at all

The result table is ordered so that smaller intersection geometries appear first, followed by larger remaining areas.

## Installation

1. Install dependencies:
```bash
pip install -r requirements_spatial.txt
```

2. Ensure you have access to a PostgreSQL database with PostGIS extension enabled.

## Usage

### Command Line Interface

```bash
python spatial_intersection_processor.py \
    --host localhost \
    --port 5432 \
    --database your_database \
    --user your_username \
    --password your_password \
    --table1 surfacemanagementagency_saltontrough \
    --table1-id-col SMA_ID \
    --table1-admin-col ADMIN_AGEN \
    --table1-geom-col geom \
    --table2 federal_american_indian_reservations \
    --table2-name-col NAME \
    --table2-geom-col geom \
    --output-table admin_table2 \
    --srid 4326
```

### Python API

```python
from spatial_intersection_processor import SpatialIntersectionProcessor

# Connection parameters
connection_params = {
    'host': 'localhost',
    'port': '5432',
    'database': 'your_database',
    'user': 'your_username',
    'password': 'your_password'
}

# Table parameters
table1_name = 'surfacemanagementagency_saltontrough'
table1_id_col = 'SMA_ID'
table1_admin_col = 'ADMIN_AGEN'
table1_geom_col = 'geom'

table2_name = 'federal_american_indian_reservations'
table2_name_col = 'NAME'
table2_geom_col = 'geom'

output_table = 'admin_table2'
srid = 4326

# Execute intersection
with SpatialIntersectionProcessor(connection_params) as processor:
    success = processor.execute_intersection(
        table1_name, table1_id_col, table1_admin_col, table1_geom_col,
        table2_name, table2_name_col, table2_geom_col,
        output_table, srid
    )
```

## Output Table Structure

The output table contains these columns:

- `id`: Auto-incrementing row identifier
- `table1_id`: ID from the first table (or NULL if no overlap)
- `table1_admin`: Administrative information from first table (or NULL if no overlap)
- `table2_name`: Name from the second table (or NULL if no overlap)
- `geom`: The geometry (intersection, remaining, or no-overlap area)
- `geom_type`: Type of geometry ('intersection', 'table1_remaining', 'table2_remaining', 'table1_no_overlap')

## Example Use Cases

### Original Use Case (BLM + Reservations)
- **Table 1**: BLM surface management areas
- **Table 2**: Federal American Indian reservations
- **Result**: Proper layering for cartographic display

### Other Potential Uses
- **Table 1**: Zoning districts
- **Table 2**: Protected areas
- **Result**: Zoning areas with protected area overlays

- **Table 1**: Land ownership parcels
- **Table 2**: Conservation easements
- **Result**: Parcels with easement overlays

## Requirements

- Python 3.6+
- PostgreSQL with PostGIS extension
- psycopg2 library
- Spatial data in both input tables

## Error Handling

The script includes comprehensive error handling:
- Database connection failures
- Missing tables or columns
- Invalid geometries
- Spatial operation failures
- Transaction rollback on errors

## Performance Considerations

- The script creates spatial indexes on the output table
- Uses `ST_MakeValid()` to ensure geometry validity
- Performs `ANALYZE` on the output table for query optimization
- Consider running during low-traffic periods for large datasets

## Troubleshooting

### Common Issues

1. **Geometry Column Not Found**: Ensure the geometry column name is correct
2. **Invalid Geometries**: The script handles this with `ST_MakeValid()`
3. **Permission Errors**: Ensure the database user has CREATE and INSERT privileges
4. **Memory Issues**: For very large datasets, consider processing in smaller chunks

### Debug Mode

Enable debug logging by modifying the logging level in the script:
```python
logging.basicConfig(level=logging.DEBUG, ...)
```
