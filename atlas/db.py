import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

def get_conn():
    """
    Create and return a new PostgreSQL connection.

    Responsibility:
    - Read credentials from environment
    - Establish DB connection
    - Do NOT handle retries or business logic
    """

    return psycopg2.connect(
        host = os.getenv("DB_HOST"),
        port = os.getenv("DB_PORT"),
        database = os.getenv("DB_NAME"),
        user = os.getenv("DB_USER"),
        password = os.getenv("DB_PASSWORD")
    )
def init_schema():
    """
    Initialize core database schema.

    This function:
    - Creates tables if they do not exist
    - Fails fast on error
    - Uses context managers to guarantee cleanup
    """

    with get_conn() as conn:
        with conn.cursor() as cur:
    
        # Bronze / Raw layer
            cur.execute("""
            CREATE TABLE IF NOT EXISTS raw_data (
                        id BiGSERIAL PRIMARY KEY,
                        source_id TEXT NOT NULL,
                        payload JSONB NOT NULL,
                        extracted_at TIMESTAMP NOT NULL
                        )

                        """)
            

            # Gold / Analytics layer
            cur.execute("""
            CREATE TABLE IF NOT EXISTS fact_records (
                        source_id TEXT PRIMARY KEY,
                        created_at TIMESTAMP NOT NULL,
                        amount Numeric
                        )
                        """)
            
            # Operational metadata
            cur.execute("""
            CREATE TABLE IF NOT EXISTS pipeline_state (
                        pipeline_name TEXT PRIMARY KEY,
                        last_processed_id TEXT)
                        """)
            conn.commit()
            cur.close()

def init_indexes():
   """
    Create indexes and constraints to improve
    performance and data correctness.
    """
   with get_conn() as conn:
        with conn.cursor() as curr:
            curr.execute("""
            Create index if not exists idx_source_id on raw_data (source_id)
                         """)
            curr.execute("""
            Create index if not exists idx_raw_data_extracted_at on raw_data (extracted_at)

            """)
            curr.execute("""
            Alter table fact_records
            alter column source_id set not null
                         """)
            
def check_db_health():
    """
    Verify databse readiness

    checks:
    - can connect
    - can run query
    - rewuired tables exits

    Rasies exceptions on failure
    """
    required_tables ={
        "raw_data",
        "fact_records",
        "pipeline_state"
    }

    with get_conn() as conn:
        with conn.cursor() as cur:
             
            # basic connectivity check:
            cur.execute("SELECT 1")

            # schema validation

            cur.execute("""
             SELECT table_name
                from information_schema.tables
                where table_schema = 'PUBLIC'
                         """)
            existing_tables = {row[0] for row in cur.fetchall()}
            missing_tables = required_tables - existing_tables

            if missing_tables:
                raise RuntimeError(f"Missing required tables: {missing_tables}")