import boto3
import gzip
import json
import os
import duckdb
import multiprocessing
from botocore import UNSIGNED
from botocore.config import Config

# --- CONFIGURATION ---
# Your specific Concept IDs
TARGET_CONCEPT_IDS = ['C15151743', 'C69562835', 'C190743605', 'C28225019', 'C152662350']
DB_PATH = "openalex_local.db"
NUM_PROCESSES = 12
# ---------------------

def get_db_connection():
    con = duckdb.connect(DB_PATH)
    con.execute("SET memory_limit='32GB'") 
    return con

def init_db(con):
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
    con.execute("CREATE TABLE IF NOT EXISTS citations (source_id VARCHAR, target_id VARCHAR)")
    con.execute("CREATE TABLE IF NOT EXISTS vector_cache (paper_id VARCHAR PRIMARY KEY, embedding FLOAT[])")

def safe_get_author(work):
    try:
        return work['authorships'][0]['author']['display_name']
    except (KeyError, IndexError, TypeError):
        return None

def safe_get_venue(work):
    try:
        return work['primary_location']['source']['display_name']
    except (KeyError, TypeError):
        return None

def process_single_file(s3_key):
    """
    Worker function: Downloads specific S3 key, filters it, returns DATA.
    Does NOT connect to DB.
    """
    # Unique temp file for this process (to avoid conflicts)
    pid = os.getpid()
    local_filename = f"temp_chunk_{pid}.gz"
    
    found_works = []
    found_citations = []
    target_set = set(TARGET_CONCEPT_IDS)
    
    # Re-initialize S3 client per process (boto3 is not thread-safe if shared)
    s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
    
    try:
        # Download
        s3.download_file("openalex", s3_key, local_filename)
        
        with gzip.open(local_filename, 'rt', encoding='utf-8') as f:
            for line in f:
                try:
                    work = json.loads(line)
                    
                    # Filter Check
                    work_concepts = set(c['id'].split('/')[-1] for c in work.get('concepts', []))
                    
                    if not work_concepts.isdisjoint(target_set):
                        work_id = work['id'].split('/')[-1]
                        
                        # Collect Work Data
                        found_works.append((
                            work_id,
                            work.get('doi'),
                            work.get('title'),
                            work.get('publication_year'),
                            work.get('cited_by_count'),
                            safe_get_author(work),
                            safe_get_venue(work),
                            json.dumps(work.get('abstract_inverted_index'))
                        ))
                        
                        # Collect Citations
                        for ref in work.get('referenced_works', []):
                            found_citations.append((work_id, ref.split('/')[-1]))
                            
                except Exception:
                    continue
                    
    except Exception as e:
        print(f"Error in worker {pid} on file {s3_key}: {e}")
    finally:
        # Cleanup temp file
        if os.path.exists(local_filename):
            os.remove(local_filename)
            
    return (found_works, found_citations)

def main():
    # 1. Setup DB (Main Process Only)
    con = get_db_connection()
    init_db(con)
    
    # 2. List all files first
    print("Listing files from OpenAlex S3...")
    s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket="openalex", Prefix="data/works/")
    
    all_keys = []
    for page in pages:
        for obj in page.get('Contents', []):
            if obj['Key'].endswith(".gz"):
                all_keys.append(obj['Key'])
    
    print(f"Found {len(all_keys)} files. Launching {NUM_PROCESSES} workers...")
    
    total_papers = 0
    files_done = 0
    
    # 3. Parallel Processing
    # imap_unordered yields results as soon as they are ready
    with multiprocessing.Pool(processes=NUM_PROCESSES) as pool:
        for works, citations in pool.imap_unordered(process_single_file, all_keys):
            files_done += 1
            
            # 4. Insert into DB (Main Process is the only writer)
            if works:
                con.executemany("INSERT OR IGNORE INTO works VALUES (?, ?, ?, ?, ?, ?, ?, ?)", works)
                total_papers += len(works)
                # Show explicit progress when we find something
                print(f"[{files_done}/{len(all_keys)}] Found +{len(works)} papers. Total: {total_papers}")
            
            if citations:
                con.executemany("INSERT INTO citations VALUES (?, ?)", citations)
            
            # Periodic status update even if empty
            if files_done % 10 == 0:
                print(f"Processed {files_done}/{len(all_keys)} files...")

    # 5. Finalize
    print("Creating Search Index...")
    try:
        con.execute("INSTALL fts; LOAD fts;")
        con.execute("PRAGMA create_fts_index('works', 'id', 'title')")
    except:
        pass

    con.close()
    print(f"Done! Total papers: {total_papers}")

if __name__ == "__main__":
    main()