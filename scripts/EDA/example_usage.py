#!/usr/bin/env python3
"""
Example usage of the SpatialIntersectionProcessor

This script demonstrates how to use the parameterized spatial intersection processor
to recreate the original admin_table2 logic.
"""

from spatial_intersection_processor import SpatialIntersectionProcessor

def main():
    """Example usage with the original table names from the SQL."""
    
    # Database connection parameters - modify these for your environment
    connection_params = {
        'host': 'localhost',
        'port': '5432',
        'database': 'lithiumvalley',
        'user': 'postgres',
        'password': '1123'
    }
    
    # Table parameters matching the original SQL
    table1_name = 'surfacemanagementagency_saltontrough'
    table1_id_col = 'SMA_ID'
    table1_admin_col = 'ADMIN_AGEN'
    table1_geom_col = 'geom'
    
    table2_name = 'blm_ca_geothermal_leases_polygon'
    table2_name_col = 'serial_nr_'
    table2_geom_col = 'geom'
    

    output_table = 'admin_table2'
    srid = 4326  # WGS84
    
    try:
        with SpatialIntersectionProcessor(connection_params) as processor:
            # Get table information to verify structure
            print("Getting table information...")
            table1_info = processor.get_table_info(table1_name)
            table2_info = processor.get_table_info(table2_name)
            
            if table1_info:
                print(f"Table 1 ({table1_name}) columns:")
                for col in table1_info['columns']:
                    print(f"  - {col['column_name']}: {col['data_type']}")
            else:
                print(f"Could not get info for table {table1_name}")
                
            if table2_info:
                print(f"\nTable 2 ({table2_name}) columns:")
                for col in table2_info['columns']:
                    print(f"  - {col['column_name']}: {col['data_type']}")
            else:
                print(f"Could not get info for table {table2_name}")
            
            # Execute the spatial intersection
            print(f"\nExecuting spatial intersection...")
            success = processor.execute_intersection(
                table1_name, table1_id_col, table1_admin_col, table1_geom_col,
                table2_name, table2_name_col, table2_geom_col,
                output_table, srid
            )
            
            if success:
                print(f"Successfully created table {output_table}")
                print("The table contains:")
                print("  - Intersection areas (smaller overlapping geometries)")
                print("  - BLM areas minus reservation overlaps (remaining larger areas)")
                print("  - Reservation areas minus BLM overlaps (remaining reservation areas)")
                print("  - BLM areas with no reservation overlap at all")
            else:
                print("Failed to create table")
                
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    main()
