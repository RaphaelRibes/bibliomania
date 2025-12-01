from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Optional
import duckdb
import logging
from pydantic import BaseModel

from database import get_db_connection
from compute_core import device_info
from semantic_engine import semantic_engine

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Prometheus Local API", version="0.1.0")

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # Allow all for local dev
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Models
class Paper(BaseModel):
    id: str
    title: str
    year: Optional[int] = None
    first_author: Optional[str] = None
    venue: Optional[str] = None
    cited_by_count: Optional[int] = None

class GraphResponse(BaseModel):
    nodes: List[Paper]
    edges: List[dict] # {source: str, target: str}

@app.get("/status")
async def get_status():
    """
    Returns the hardware utilization and system status.
    """
    return {
        "hardware": device_info(),
        "status": "online"
    }

@app.get("/search", response_model=List[Paper])
async def search_papers(q: str = Query(..., min_length=3)):
    """
    Finds papers by Title OR DOI.
    """
    con = get_db_connection(read_only=True)
    papers = []
    try:
        # 1. First, check if the query looks like a DOI or ID directly
        # DOIs often contain '10.' and '/'
        if "10." in q and "/" in q:
            doi_query = """
            SELECT id, title, publication_year, first_author, venue, cited_by_count
            FROM works
            WHERE doi ILIKE ?
            LIMIT 1
            """
            # We strip whitespace just in case
            r = con.execute(doi_query, [q.strip()]).fetchone()
            if r:
                papers.append(Paper(
                    id=r[0], title=r[1], year=r[2], first_author=r[3], venue=r[4], cited_by_count=r[5]
                ))
                return papers

        # 2. If not found or not a DOI, use FTS on Title
        # We explicitly search against the 'title' column in the FTS index if possible,
        # or use the generic match function.
        fts_query = """
        SELECT id, title, publication_year, first_author, venue, cited_by_count
        FROM works
        WHERE fts_main_works.match_bm25(id, ?) IS NOT NULL
        ORDER BY fts_main_works.match_bm25(id, ?) DESC
        LIMIT 20
        """
        results = con.execute(fts_query, [q, q]).fetchall()
        
        for r in results:
            papers.append(Paper(
                id=r[0], title=r[1], year=r[2], first_author=r[3], venue=r[4], cited_by_count=r[5]
            ))
            
        return papers

    except Exception as e:
        logger.error(f"Search failed: {e}")
        # Fallback to standard SQL ILIKE if FTS fails
        try:
            fallback_query = """
            SELECT id, title, publication_year, first_author, venue, cited_by_count
            FROM works
            WHERE title ILIKE ?
            LIMIT 20
            """
            results = con.execute(fallback_query, [f"%{q}%"]).fetchall()
            for r in results:
                papers.append(Paper(
                    id=r[0], title=r[1], year=r[2], first_author=r[3], venue=r[4], cited_by_count=r[5]
                ))
            return papers
        except Exception as e2:
            raise HTTPException(status_code=500, detail=str(e2))
    finally:
        con.close()

@app.get("/graph/expand", response_model=GraphResponse)
async def expand_graph(paper_id: str):
    """
    Fetches references (outgoing) and citations (incoming) for a given paper.
    Also fetches similar papers using the Semantic Engine.
    """
    con = get_db_connection(read_only=True)
    nodes = {}
    edges = []
    
    try:
        # 1. Get the central paper details
        central_res = con.execute("SELECT id, title, publication_year, first_author, venue, cited_by_count, abstract_inverted_index FROM works WHERE id = ?", [paper_id]).fetchone()
        if not central_res:
            raise HTTPException(status_code=404, detail="Paper not found")
            
        central_paper = {
            "id": central_res[0],
            "title": central_res[1],
            "year": central_res[2],
            "first_author": central_res[3],
            "venue": central_res[4],
            "cited_by_count": central_res[5],
            "abstract": str(central_res[6]) # Simplified abstract handling
        }
        nodes[paper_id] = central_paper
        
        # 2. Get References (Outgoing) - Limit to top 20 by citation count if possible, or just random
        refs_query = """
        SELECT w.id, w.title, w.publication_year, w.first_author, w.venue, w.cited_by_count
        FROM citations c
        JOIN works w ON c.target_id = w.id
        WHERE c.source_id = ?
        LIMIT 20
        """
        refs = con.execute(refs_query, [paper_id]).fetchall()
        for r in refs:
            pid = r[0]
            if pid not in nodes:
                nodes[pid] = {
                    "id": pid, "title": r[1], "year": r[2], "first_author": r[3], "venue": r[4], "cited_by_count": r[5]
                }
            edges.append({"source": paper_id, "target": pid})

        # 3. Get Citations (Incoming)
        cited_query = """
        SELECT w.id, w.title, w.publication_year, w.first_author, w.venue, w.cited_by_count
        FROM citations c
        JOIN works w ON c.source_id = w.id
        WHERE c.target_id = ?
        LIMIT 20
        """
        cited = con.execute(cited_query, [paper_id]).fetchall()
        for r in cited:
            pid = r[0]
            if pid not in nodes:
                nodes[pid] = {
                    "id": pid, "title": r[1], "year": r[2], "first_author": r[3], "venue": r[4], "cited_by_count": r[5]
                }
            edges.append({"source": pid, "target": paper_id})
            
        # 4. Semantic Similarity
        # We need to generate embedding for the central paper if not exists
        # And then find similar papers
        # Note: This might be slow on first run as it loads the model
        try:
            # We pass a list with one paper to ensure it's in cache
            semantic_engine.get_embeddings([central_paper])
            similar_papers = semantic_engine.calculate_similarity(paper_id, limit=5)
            
            for sim in similar_papers:
                pid = sim['id']
                # Fetch details
                if pid not in nodes:
                    r = con.execute("SELECT id, title, publication_year, first_author, venue, cited_by_count FROM works WHERE id = ?", [pid]).fetchone()
                    if r:
                        nodes[pid] = {
                            "id": r[0], "title": r[1], "year": r[2], "first_author": r[3], "venue": r[4], "cited_by_count": r[5]
                        }
                        # Add a special edge type or just a link?
                        # For now, we just add the node. Or maybe a "similarity" edge.
                        # Let's add an edge to visualize it, maybe with a different type in frontend
                        edges.append({"source": paper_id, "target": pid, "type": "similarity"})
        except Exception as e:
            logger.warning(f"Semantic search failed: {e}")

    finally:
        con.close()
        
    return GraphResponse(
        nodes=[Paper(**p) for p in nodes.values()],
        edges=edges
    )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
