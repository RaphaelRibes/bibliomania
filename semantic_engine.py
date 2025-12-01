import torch
from transformers import AutoTokenizer
from adapters import AutoAdapterModel
import duckdb
import logging
import numpy as np
from typing import List, Dict, Any
from compute_core import get_device
from database import get_db_connection

logger = logging.getLogger(__name__)

class SemanticEngine:
    def __init__(self):
        self.device = get_device()
        self.tokenizer = None
        self.model = None
        self.is_loaded = False

    def load_model(self):
        """
        Lazy loads the SPECTER2 model and adapter.
        """
        if self.is_loaded:
            return

        logger.info(f"Loading SPECTER2 model on {self.device}...")
        try:
            self.tokenizer = AutoTokenizer.from_pretrained('allenai/specter2_base')
            self.model = AutoAdapterModel.from_pretrained('allenai/specter2_base')
            self.model.load_adapter("allenai/specter2", source="hf", set_active=True)
            self.model.to(self.device)
            self.model.eval()
            self.is_loaded = True
            logger.info("SPECTER2 model loaded successfully.")
        except Exception as e:
            logger.error(f"Failed to load SPECTER2 model: {e}")
            raise

    def get_embeddings(self, papers: List[Dict[str, str]]) -> Dict[str, np.ndarray]:
        """
        Generates embeddings for a list of papers (id, title, abstract).
        Checks cache first.
        """
        self.load_model()
        con = get_db_connection()
        
        paper_ids = [p['id'] for p in papers]
        
        # Check cache
        # We need to format the list for SQL IN clause
        ids_str = ",".join([f"'{pid}'" for pid in paper_ids])
        if not ids_str:
            return {}
            
        cached_res = con.execute(f"SELECT paper_id, embedding FROM vector_cache WHERE paper_id IN ({ids_str})").fetchall()
        embeddings = {row[0]: np.array(row[1]) for row in cached_res}
        
        # Identify missing
        missing_papers = [p for p in papers if p['id'] not in embeddings]
        
        if missing_papers:
            logger.info(f"Generating embeddings for {len(missing_papers)} papers...")
            
            # Batch processing could be added here for large lists
            texts = [p.get('title', '') + ' ' + p.get('abstract', '') for p in missing_papers]
            
            # Tokenize
            inputs = self.tokenizer(texts, padding=True, truncation=True, return_tensors="pt", max_length=512).to(self.device)
            
            with torch.no_grad():
                outputs = self.model(**inputs)
                # Take the first token (CLS token) as the embedding
                new_embeddings = outputs.last_hidden_state[:, 0, :].cpu().numpy()
            
            # Store in cache and result dict
            for i, paper in enumerate(missing_papers):
                emb = new_embeddings[i]
                embeddings[paper['id']] = emb
                # Insert into cache
                # DuckDB array insertion needs careful formatting or parameter binding
                # Using executemany with parameters is safer
                con.execute("INSERT OR REPLACE INTO vector_cache VALUES (?, ?)", [paper['id'], emb.tolist()])
        
        con.close()
        return embeddings

    def calculate_similarity(self, seed_id: str, limit: int = 20) -> List[Dict[str, Any]]:
        """
        Finds similar papers to the seed paper using cosine similarity on cached vectors.
        """
        con = get_db_connection()
        
        # Get seed embedding
        res = con.execute("SELECT embedding FROM vector_cache WHERE paper_id = ?", [seed_id]).fetchone()
        if not res:
            logger.warning(f"No embedding found for seed {seed_id}. Cannot calculate similarity.")
            return []
        
        seed_vector = res[0] # This is a list/array
        
        # Use DuckDB's array_cosine_similarity
        # We cast the seed vector to FLOAT[] explicitly in the query
        query = """
        SELECT 
            paper_id, 
            array_cosine_similarity(embedding, ?::FLOAT[]) as score
        FROM vector_cache
        WHERE paper_id != ?
        ORDER BY score DESC
        LIMIT ?
        """
        
        results = con.execute(query, [seed_vector, seed_id, limit]).fetchall()
        con.close()
        
        return [{"id": r[0], "score": r[1]} for r in results]

# Singleton instance
semantic_engine = SemanticEngine()
