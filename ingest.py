import duckdb
import logging
import argparse
import os
from database import get_db_connection

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def ingest_works(snapshot_path: str, con: duckdb.DuckDBPyConnection):
    """
    Ingests Works from OpenAlex snapshot (JSONL.gz) into DuckDB.
    """
    logger.info("Starting Works ingestion...")
    
    # Define the path pattern for works
    works_path = os.path.join(snapshot_path, "data/works/*/*.gz")
    
    # We use a COPY statement to read JSON and write to a Parquet file first for efficiency, 
    # or directly into the table if we want to skip the intermediate parquet step for this MVP.
    # However, the plan mentioned creating 'works.parquet'. Let's follow the efficient path.
    # But for simplicity in this script, we will insert into the 'works' table we defined in database.py.
    # If the dataset is huge, writing to Parquet first is better.
    
    # Let's try to insert directly into the table using the optimized query from the plan.
    # Note: We need to map the JSON structure to our flat table.
    
    query = f"""
    INSERT INTO works
    SELECT
        id,
        doi,
        title,
        publication_year,
        cited_by_count,
        authorships[1].author.display_name as first_author,
        primary_location.source.display_name as venue,
        abstract_inverted_index
    FROM read_json_auto('{works_path}', hive_partitioning=1)
    WHERE title IS NOT NULL
      AND type IN ('article', 'preprint', 'proceedings-article')
    """
    
    try:
        con.execute(query)
        logger.info("Works ingestion completed.")
    except Exception as e:
        logger.error(f"Error during Works ingestion: {e}")

def ingest_citations(snapshot_path: str, con: duckdb.DuckDBPyConnection):
    """
    Ingests Citations (Edges) from OpenAlex snapshot.
    """
    logger.info("Starting Citations ingestion...")
    works_path = os.path.join(snapshot_path, "data/works/*/*.gz")
    
    # We need to unnest the referenced_works array
    query = f"""
    INSERT INTO citations
    SELECT
        id as source_id,
        UNNEST(referenced_works) as target_id
    FROM read_json_auto('{works_path}', hive_partitioning=1)
    WHERE referenced_works IS NOT NULL
    """
    
    try:
        con.execute(query)
        logger.info("Citations ingestion completed.")
    except Exception as e:
        logger.error(f"Error during Citations ingestion: {e}")

def main():
    parser = argparse.ArgumentParser(description="Ingest OpenAlex Snapshot into DuckDB")
    parser.add_argument("--snapshot", type=str, required=True, help="Path to the OpenAlex snapshot directory")
    args = parser.parse_args()
    
    if not os.path.exists(args.snapshot):
        logger.error(f"Snapshot directory not found: {args.snapshot}")
        return

    con = get_db_connection()
    
    # Create tables if they don't exist (idempotent)
    # We rely on database.py init_db logic, but here we assume tables exist or we create them.
    # Let's ensure they exist.
    from database import init_db
    init_db()
    
    # Re-connect as init_db closes connection
    con = get_db_connection()
    
    ingest_works(args.snapshot, con)
    ingest_citations(args.snapshot, con)
    
    con.close()
    logger.info("Ingestion process finished.")

if __name__ == "__main__":
    main()
