#!/usr/bin/env python3
"""
Spatial Intersection Processor

A parameterized Python script that performs spatial intersections between two tables
and creates a new table with overlapping geometries on top and remaining larger areas below.
"""

import psycopg2
import psycopg2.extras
from psycopg2.extras import RealDictCursor
import logging
from typing import Dict, List, Optional, Tuple
import argparse
import sys

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class SpatialIntersectionProcessor:
    """Handles spatial intersection processing between two tables."""
    
    def __init__(self, connection_params: Dict[str, str]):
        """
        Initialize the processor with database connection parameters.
        
        Args:
            connection_params: Dictionary with keys: host, port, database, user, password
        """
        self.connection_params = connection_params
        self.conn = None
        
    def connect(self):
        """Establish database connection."""
        try:
            self.conn = psycopg2.connect(**self.connection_params)
            logger.info("Database connection established")
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise
            
    def disconnect(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")
            
    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()
        
    def generate_intersection_sql(self, 
                                 table1_name: str,
                                 table1_id_col: str,
                                 table1_admin_col: str,
                                 table1_geom_col: str,
                                 table2_name: str,
                                 table2_name_col: str,
                                 table2_geom_col: str,
                                 output_table: str,
                                 srid: int = 4326) -> str:
        """
        Generate the SQL for spatial intersection processing.
        
        Args:
            table1_name: Name of first table (e.g., BLM surface management)
            table1_id_col: ID column name in first table
            table1_admin_col: Administrative column name in first table
            table1_geom_col: Geometry column name in first table
            table2_name: Name of second table (e.g., reservations)
            table2_name_col: Name column name in second table
            table2_geom_col: Geometry column name in second table
            output_table: Name of output table to create
            srid: Spatial reference system ID (default: 4326 for WGS84)
            
        Returns:
            Generated SQL string
        """
        
        sql = f"""
DROP TABLE IF EXISTS public.{output_table};

WITH table1_src AS (
  SELECT
    a."{table1_id_col}"::text       AS table1_id,
    a."{table1_admin_col}"::text   AS table1_admin,
    ST_Transform(ST_MakeValid(a.{table1_geom_col}), {srid})   AS geom
  FROM public.{table1_name} a
),
table2_src AS (
  SELECT
    r."{table2_name_col}"::text AS table2_name,
    ST_Transform(ST_MakeValid(r.{table2_geom_col}), {srid}) AS geom
  FROM public.{table2_name} r
  WHERE NOT ST_IsEmpty(r.{table2_geom_col})
),

-- Intersection areas (smaller overlapping geometries)
intersections AS (
  SELECT
    t1.table1_id,
    t1.table1_admin,
    t2.table2_name,
    ST_Intersection(t1.geom, t2.geom) AS geom,
    'intersection' AS geom_type
  FROM table1_src t1
  JOIN table2_src t2 ON ST_Intersects(t1.geom, t2.geom)
  WHERE NOT ST_IsEmpty(ST_Intersection(t1.geom, t2.geom))
),

-- Pre-calculate union of all table2 geometries for each table1 area
table1_table2_union AS (
  SELECT
    t1.table1_id,
    t1.table1_admin,
    t1.geom AS table1_geom,
    ST_Union(t2.geom) AS table2_union_geom
  FROM table1_src t1
  LEFT JOIN table2_src t2 ON ST_Intersects(t1.geom, t2.geom)
  GROUP BY t1.table1_id, t1.table1_admin, t1.geom
),

-- Table1 areas minus table2 overlaps (remaining larger areas)
table1_remaining AS (
  SELECT
    t1.table1_id,
    t1.table1_admin,
    NULL::text AS table2_name,
    ST_Difference(t1.table1_geom, t1.table2_union_geom) AS geom,
    'table1_remaining' AS geom_type
  FROM table1_table2_union t1
  WHERE NOT ST_IsEmpty(ST_Difference(t1.table1_geom, t1.table2_union_geom))
),

-- Pre-calculate union of all table1 geometries for each table2 area
table2_table1_union AS (
  SELECT
    t2.table2_name,
    t2.geom AS table2_geom,
    ST_Union(t1.geom) AS table1_union_geom
  FROM table2_src t2
  LEFT JOIN table1_src t1 ON ST_Intersects(t1.geom, t2.geom)
  GROUP BY t2.table2_name, t2.geom
),

-- Table2 areas minus table1 overlaps (remaining table2 areas)
table2_remaining AS (
  SELECT
    NULL::text AS table1_id,
    NULL::text AS table1_admin,
    t2.table2_name,
    ST_Difference(t2.table2_geom, t2.table1_union_geom) AS geom,
    'table2_remaining' AS geom_type
  FROM table2_table1_union t2
  WHERE NOT ST_IsEmpty(ST_Difference(t2.table2_geom, t2.table1_union_geom))
),

-- Table1 areas with no table2 overlap at all
table1_no_overlap AS (
  SELECT
    t1.table1_id,
    t1.table1_admin,
    NULL::text AS table2_name,
    t1.geom,
    'table1_no_overlap' AS geom_type
  FROM table1_src t1
  WHERE NOT EXISTS (
    SELECT 1 FROM table2_src t2 WHERE ST_Intersects(t1.geom, t2.geom)
  )
)

-- Combine all results
SELECT
  ROW_NUMBER() OVER () AS id,
  table1_id,
  table1_admin,
  table2_name,
  geom
INTO public.{output_table}
FROM (
  SELECT * FROM intersections
  UNION ALL
  SELECT * FROM table1_remaining
  UNION ALL
  SELECT * FROM table2_remaining
  UNION ALL
  SELECT * FROM table1_no_overlap
) combined
WHERE geom IS NOT NULL AND NOT ST_IsEmpty(geom);

-- Create spatial index
CREATE INDEX {output_table}_gix ON public.{output_table} USING GIST (geom);
ANALYZE public.{output_table};
"""
        return sql
        
    def execute_intersection(self, 
                            table1_name: str,
                            table1_id_col: str,
                            table1_admin_col: str,
                            table1_geom_col: str,
                            table2_name: str,
                            table2_name_col: str,
                            table2_geom_col: str,
                            output_table: str,
                            srid: int = 4326) -> bool:
        """
        Execute the spatial intersection processing.
        
        Args:
            table1_name: Name of first table
            table1_id_col: ID column name in first table
            table1_admin_col: Administrative column name in first table
            table1_geom_col: Geometry column name in first table
            table2_name: Name of second table
            table2_name_col: Name column name in second table
            table2_geom_col: Geometry column name in second table
            output_table: Name of output table to create
            srid: Spatial reference system ID
            
        Returns:
            True if successful, False otherwise
        """
        try:
            sql = self.generate_intersection_sql(
                table1_name, table1_id_col, table1_admin_col, table1_geom_col,
                table2_name, table2_name_col, table2_geom_col,
                output_table, srid
            )
            
            with self.conn.cursor() as cursor:
                logger.info(f"Executing spatial intersection for {table1_name} and {table2_name}")
                cursor.execute(sql)
                self.conn.commit()
                
            logger.info(f"Successfully created table {output_table}")
            return True
            
        except Exception as e:
            logger.error(f"Error executing spatial intersection: {e}")
            if self.conn:
                self.conn.rollback()
            return False
            
    def get_table_info(self, table_name: str) -> Optional[Dict]:
        """
        Get information about a table's structure.
        
        Args:
            table_name: Name of the table
            
        Returns:
            Dictionary with table information or None if error
        """
        try:
            with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT column_name, data_type, is_nullable
                    FROM information_schema.columns
                    WHERE table_name = %s
                    ORDER BY ordinal_position
                """, (table_name,))
                
                columns = cursor.fetchall()
                return {
                    'table_name': table_name,
                    'columns': [dict(col) for col in columns]
                }
                
        except Exception as e:
            logger.error(f"Error getting table info for {table_name}: {e}")
            return None


def main():
    """Main function for command line usage."""
    parser = argparse.ArgumentParser(description='Process spatial intersections between two tables')
    
    # Database connection parameters
    parser.add_argument('--host', default='localhost', help='Database host')
    parser.add_argument('--port', default='5432', help='Database port')
    parser.add_argument('--database', required=True, help='Database name')
    parser.add_argument('--user', required=True, help='Database user')
    parser.add_argument('--password', required=True, help='Database password')
    
    # Table parameters
    parser.add_argument('--table1', required=True, help='First table name')
    parser.add_argument('--table1-id-col', required=True, help='ID column in first table')
    parser.add_argument('--table1-admin-col', required=True, help='Administrative column in first table')
    parser.add_argument('--table1-geom-col', default='geom', help='Geometry column in first table')
    
    parser.add_argument('--table2', required=True, help='Second table name')
    parser.add_argument('--table2-name-col', required=True, help='Name column in second table')
    parser.add_argument('--table2-geom-col', default='geom', help='Geometry column in second table')
    
    parser.add_argument('--output-table', required=True, help='Output table name')
    parser.add_argument('--srid', type=int, default=4326, help='Spatial reference system ID')
    
    args = parser.parse_args()
    
    # Connection parameters
    connection_params = {
        'host': args.host,
        'port': args.port,
        'database': args.database,
        'user': args.user,
        'password': args.password
    }
    
    try:
        with SpatialIntersectionProcessor(connection_params) as processor:
            # Get table information
            table1_info = processor.get_table_info(args.table1)
            table2_info = processor.get_table_info(args.table2)
            
            if table1_info:
                logger.info(f"Table 1 ({args.table1}) columns: {[col['column_name'] for col in table1_info['columns']]}")
            if table2_info:
                logger.info(f"Table 2 ({args.table2}) columns: {[col['column_name'] for col in table2_info['columns']]}")
            
            # Execute intersection
            success = processor.execute_intersection(
                args.table1, args.table1_id_col, args.table1_admin_col, args.table1_geom_col,
                args.table2, args.table2_name_col, args.table2_geom_col,
                args.output_table, args.srid
            )
            
            if success:
                logger.info("Spatial intersection processing completed successfully")
                sys.exit(0)
            else:
                logger.error("Spatial intersection processing failed")
                sys.exit(1)
                
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
