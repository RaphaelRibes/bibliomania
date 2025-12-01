import duckdb
import logging
import os

logger = logging.getLogger(__name__)

DB_PATH = "openalex_local.db"

def get_db_connection(read_only=False):
    """
    Establishes and returns a connection to the DuckDB database.
    Configures memory limits and threading.
    """
    try:
        con = duckdb.connect(DB_PATH, read_only=read_only)
        
        # Optimization settings
        # Leave 20% for OS, but for safety in this env, let's set a conservative limit or dynamic
        # For this implementation, we'll set a reasonable default, e.g., 8GB or 12GB if available
        # In a real scenario, we'd check psutil.virtual_memory()
        
        # Setting a safe default for now. User can adjust.
        con.execute("SET memory_limit='200GB'") 
        con.execute("SET threads TO 6") # Adjust based on CPU
        
        # Enable FTS
        con.execute("INSTALL fts")
        con.execute("LOAD fts")
        
        return con
    except Exception as e:
        logger.error(f"Failed to connect to DuckDB: {e}")
        raise

def init_db():
    """
    Initializes the database schema if it doesn't exist.
    """
    con = get_db_connection()
    
    logger.info("Initializing database schema...")
    
    # Create Works table (schema matching OpenAlex simplified)
    # We use a sequence or auto-incrementing ID if needed, but OpenAlex has its own IDs (e.g., W12345)
    # We will store the OpenAlex ID as the primary key string.
    
    # Note: In the ingest phase, we might create these tables from Parquet directly.
    # But we define the schema here for reference or manual insertion.
    
    con.execute("""
        CREATE TABLE IF NOT EXISTS works (
            id VARCHAR PRIMARY KEY,
            doi VARCHAR,
            title VARCHAR,
            publication_year INTEGER,
            cited_by_count INTEGER,
            first_author VARCHAR,
            venue VARCHAR,
            abstract_inverted_index JSON
        )
    """)
    
    # Create Citations table
    con.execute("""
        CREATE TABLE IF NOT EXISTS citations (
            source_id VARCHAR,
            target_id VARCHAR
        )
    """)
    
    # Create Vector Cache table for embeddings
    con.execute("""
        CREATE TABLE IF NOT EXISTS vector_cache (
            paper_id VARCHAR PRIMARY KEY,
            embedding FLOAT[]
        )
    """)
    
    # Create FTS Index on title
    # Note: FTS indexes in DuckDB are created on existing tables. 
    # If the table is empty, it's fine.
    try:
        con.execute("PRAGMA create_fts_index('works', 'id', 'title')")
    except duckdb.CatalogException:
        # Index might already exist or table is empty/managed differently
        pass
        
    logger.info("Database initialized.")
    con.close()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    init_db()
