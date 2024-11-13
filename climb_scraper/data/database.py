import sqlite3
from datetime import datetime
import pandas as pd
from typing import Optional, List, Tuple


class ClimbingDatabase:
    def __init__(self, db_path: str = None):
        """Initialize database connection and create tables if they don't exist"""
        if db_path is None:
            import os
            # Get the project root directory
            project_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
            self.db_path = os.path.join(project_root, "climbing_data.db")
        else:
            self.db_path = db_path
        self.conn = None
        self.create_tables()
    def connect(self):
        """Create a database connection"""
        try:
            self.conn = sqlite3.connect(self.db_path)
            return self.conn
        except sqlite3.Error as e:
            print(f"Error connecting to database: {e}")
            return None

    def close(self):
        """Close the database connection"""
        if self.conn:
            self.conn.close()

    def create_tables(self):
        """Create the necessary tables if they don't exist"""
        create_tables_sql = '''
        -- Crag IDs to process table
        CREATE TABLE IF NOT EXISTS crag_ids (
            crag_id INTEGER PRIMARY KEY,
            processed BOOLEAN DEFAULT FALSE,
            date_added DATETIME DEFAULT CURRENT_TIMESTAMP,
            date_processed DATETIME
        );

        -- Crags table
        CREATE TABLE IF NOT EXISTS crags (
            crag_id INTEGER PRIMARY KEY,
            name TEXT,
            latitude REAL,
            longitude REAL,
            date_added DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (crag_id) REFERENCES crag_ids (crag_id)
        );

        -- Climbs table
        CREATE TABLE IF NOT EXISTS climbs (
            climb_id INTEGER PRIMARY KEY,
            crag_id INTEGER,
            name TEXT NOT NULL,
            grade TEXT,
            tech_grade TEXT,
            grade_score REAL,
            grade_type INTEGER,
            date_added DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (crag_id) REFERENCES crags (crag_id)
        );
        '''

        try:
            conn = self.connect()
            if conn:
                # Split the SQL commands and execute them separately
                for command in create_tables_sql.split(';'):
                    if command.strip():
                        conn.execute(command)
                conn.commit()
                self.close()
        except sqlite3.Error as e:
            print(f"Error creating tables: {e}")


    def add_crag_ids(self, start_id: int, end_id: int) -> bool:
        """Add a range of crag IDs to be processed"""
        try:
            conn = self.connect()
            if conn:
                sql = '''
                INSERT OR IGNORE INTO crag_ids (crag_id, processed)
                VALUES (?, FALSE)
                '''
                ids = [(i,) for i in range(start_id, end_id + 1)]
                conn.executemany(sql, ids)
                conn.commit()
                self.close()
                return True
        except sqlite3.Error as e:
            print(f"Error adding crag IDs: {e}")
            return False

    def get_next_unprocessed_crag_id(self) -> Optional[int]:
        """Get the next crag ID that hasn't been processed yet"""
        try:
            conn = self.connect()
            if conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT crag_id 
                    FROM crag_ids 
                    WHERE processed = FALSE 
                    ORDER BY crag_id 
                    LIMIT 1
                """)
                result = cursor.fetchone()
                self.close()
                return result[0] if result else None
        except sqlite3.Error as e:
            print(f"Error getting next crag ID: {e}")
            return None

    def mark_crag_processed(self, crag_id: int, success: bool = True) -> bool:
        """Mark a crag ID as processed"""
        try:
            conn = self.connect()
            if conn:
                sql = '''
                UPDATE crag_ids 
                SET processed = ?, date_processed = CURRENT_TIMESTAMP
                WHERE crag_id = ?
                '''
                conn.execute(sql, (success, crag_id))
                conn.commit()
                self.close()
                return True
        except sqlite3.Error as e:
            print(f"Error marking crag as processed: {e}")
            return False

    def get_processing_stats(self) -> Tuple[int, int]:
        """Get counts of processed and unprocessed crags"""
        try:
            conn = self.connect()
            if conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT 
                        SUM(CASE WHEN processed = TRUE THEN 1 ELSE 0 END) as processed,
                        SUM(CASE WHEN processed = FALSE THEN 1 ELSE 0 END) as unprocessed
                    FROM crag_ids
                """)
                result = cursor.fetchone()
                self.close()
                return result if result else (0, 0)
        except sqlite3.Error as e:
            print(f"Error getting processing stats: {e}")
            return (0, 0)

    def insert_crag(self, crag_id: int, name: Optional[str] = None,
                    latitude: Optional[float] = None, longitude: Optional[float] = None) -> bool:
        """Insert a new crag into the database"""
        try:
            conn = self.connect()
            if conn:
                sql = '''
                INSERT OR IGNORE INTO crags (crag_id, name, latitude, longitude)
                VALUES (?, ?, ?, ?)
                '''
                conn.execute(sql, (crag_id, name, latitude, longitude))
                conn.commit()
                self.close()
                return True
        except sqlite3.Error as e:
            print(f"Error inserting crag: {e}")
            return False

    def insert_climbs(self, climbs_df: pd.DataFrame, crag_id: int) -> bool:
        """Insert climbs from a dataframe into the database"""
        try:
            conn = self.connect()
            if conn:
                # Prepare the data for insertion
                climbs_data = []
                for _, row in climbs_df.iterrows():
                    climb_data = (
                        row['id'],  # climb_id from the original data
                        crag_id,
                        row['name'],  # climb name from the original data
                        row['grade'],
                        row['techgrade'],
                        row['gradescore'],
                        row['gradetype']
                    )
                    climbs_data.append(climb_data)

                # Insert the data
                sql = '''
                INSERT OR REPLACE INTO climbs 
                (climb_id, crag_id, name, grade, tech_grade, grade_score, grade_type)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                '''
                conn.executemany(sql, climbs_data)
                conn.commit()
                self.close()
                return True
        except sqlite3.Error as e:
            print(f"Error inserting climbs: {e}")
            return False
        except Exception as e:
            print(f"Error processing climb data: {e}")
            print("DataFrame columns:", climbs_df.columns)
            return False

    def reset_crag_ids(self, start_id: int) -> bool:
        """Reset the crag_ids table and start fresh from a specific ID"""
        try:
            conn = self.connect()
            if conn:
                # First, delete all records from dependent tables due to foreign key constraints
                conn.execute("DELETE FROM climbs")
                conn.execute("DELETE FROM crags")
                conn.execute("DELETE FROM crag_ids")

                # Then add the new starting range (e.g., from 200 to 200 + 50)
                sql = '''
                INSERT INTO crag_ids (crag_id, processed)
                VALUES (?, FALSE)
                '''
                # Add first batch of IDs
                ids = [(i,) for i in range(start_id, start_id + 50)]
                conn.executemany(sql, ids)

                conn.commit()
                self.close()
                print(f"Database reset successfully. Starting from ID {start_id}")
                return True
        except sqlite3.Error as e:
            print(f"Error resetting database: {e}")
            return False

    def verify_database_state(self):
        """Print current database state"""
        try:
            conn = self.connect()
            if conn:
                cursor = conn.cursor()

                # Get counts
                cursor.execute("SELECT COUNT(*) FROM crag_ids")
                total_ids = cursor.fetchone()[0]

                cursor.execute("SELECT COUNT(*) FROM crag_ids WHERE processed = 1")
                processed = cursor.fetchone()[0]

                cursor.execute("SELECT MIN(crag_id), MAX(crag_id) FROM crag_ids")
                min_max = cursor.fetchone()

                print("\n=== Database Status ===")
                print(f"Total crag IDs: {total_ids}")
                print(f"Processed: {processed}")
                print(f"Unprocessed: {total_ids - processed}")
                print(f"ID range: {min_max[0]} to {min_max[1]}")
                print("=====================\n")

                self.close()
        except Exception as e:
            print(f"Error verifying database state: {e}")
