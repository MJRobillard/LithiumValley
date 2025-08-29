# Admin Table 2 Visualization

This directory contains scripts to create live connections to the PostgreSQL database and visualize the `admin_table2` data on interactive maps.

## Overview

The `admin_table2` table contains:
- **BLM Areas**: Surface management agency areas with administrative information
- **Reservations**: Federal American Indian reservation areas
- **Geometry**: Spatial data in EPSG:4326 (WGS84) coordinate system

## Scripts

### 1. Interactive Web Map (`visualize_admin_table2.py`)

Creates an interactive HTML map using Folium that opens in your web browser.

**Features:**
- Live connection to PostgreSQL
- Interactive map with multiple tile layers
- Color-coded BLM areas by administrative agency
- Highlighted reservation areas
- Clickable features with detailed popups
- Layer controls to toggle different feature types
- Fullscreen and measurement tools

**Usage:**
```bash
python scripts/visualize_admin_table2.py
```

**Requirements:**
- `folium` package (added to requirements.txt)
- Web browser for viewing

### 2. Static Map (`visualize_admin_table2_matplotlib.py`)

Creates a static PNG map using matplotlib for environments without web browsers.

**Features:**
- Live connection to PostgreSQL
- Static map with color-coded features
- Legend showing administrative types
- Statistics box with feature counts
- High-resolution PNG output

**Usage:**
```bash
python scripts/visualize_admin_table2_matplotlib.py
```

**Requirements:**
- `matplotlib` package (already in requirements.txt)

## Database Connection

Both scripts use the same connection parameters:
- **Host**: localhost
- **Port**: 5432
- **Database**: lithiumvalley
- **User**: postgres
- **Password**: 1123

To modify these, edit the `DEFAULT_*` constants in each script.

## Output Files

- **Interactive map**: `outputs/admin_table2_map.html`
- **Static map**: `outputs/admin_table2_map.png`

## Troubleshooting

### Common Issues

1. **Connection Error**: Ensure PostgreSQL is running and accessible
2. **Missing Table**: Verify `admin_table2` exists in the `public` schema
3. **Geometry Errors**: Check that geometries are valid and not empty
4. **Missing Dependencies**: Install required packages with `pip install -r requirements.txt`

### Data Structure

The `admin_table2` table should have these columns:
- `id`: Row identifier
- `blm_id`: BLM identifier
- `blm_admin`: Administrative agency (BLM, USFS, NPS, FWS, DOD)
- `res_name`: Reservation name (NULL for BLM areas)
- `geom`: Geometry column (PostGIS geometry type)

## Example Output

The visualization will show:
- **Blue areas**: BLM-managed lands
- **Green areas**: US Forest Service lands
- **Purple areas**: National Park Service lands
- **Orange areas**: Fish & Wildlife Service lands
- **Red areas**: Department of Defense lands
- **Yellow/Orange areas**: American Indian reservations

## Performance Notes

- Large datasets may take time to load and render
- Consider using spatial filters in the SQL query for very large tables
- The interactive map loads all data into memory for client-side rendering
