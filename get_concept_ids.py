import requests

def get_concept_id(term):
    url = f"https://api.openalex.org/concepts?search={term}"
    try:
        r = requests.get(url).json()
        results = r.get('results', [])
        if results:
            best_match = results[0]
            print(f"Found: {best_match['display_name']} -> {best_match['id']}")
            return best_match['id'].replace("https://openalex.org/", "")
        else:
            print(f"No concept found for '{term}'")
            return None
    except Exception as e:
        print(f"Error searching for {term}: {e}")
        return None

# Define your fields of interest here
search_terms = [
    "Metagenomics",
    "Microbial ecology", 
    "Virome",
    "Biological network",
    "Systems biology"  # Good for "networks"
]

print("--- Fetc hing Concept IDs ---")
found_ids = []
for term in search_terms:
    cid = get_concept_id(term)
    if cid:
        found_ids.append(cid)

print("\nCopy this list into your ingest script:")
print(f"TARGET_CONCEPT_IDS = {found_ids}")