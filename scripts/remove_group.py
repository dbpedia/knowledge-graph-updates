"""
Description: Handy script for removing a group and all artifacts/versions associated with it.
Author: Milan Dojchinovski
Email: dojcinovski.milan@gmail.com
Date: 2025-06-09
License: CC BY 4.0
"""

import requests
from SPARQLWrapper import SPARQLWrapper, JSON

# Configuration
DATABUS_BASE = "https://databus.dbpedia.org"
USER = "m1ci"
GROUP = "dnb"  # Replace with your actual group name
API_KEY = "YOUR KEY"   # Replace with your API key

# SPARQL endpoint
SPARQL_ENDPOINT = "https://databus.dbpedia.org/sparql"

HEADERS = {
    'accept': 'application/json',
    'X-API-KEY': API_KEY,
    'Content-Type': 'application/ld+json'
}

def query_sparql(query):
    sparql = SPARQLWrapper(SPARQL_ENDPOINT)
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    results = sparql.query().convert()
    return results["results"]["bindings"]

def get_artifacts(group):
    query = f"""
    PREFIX databus: <https://dataid.dbpedia.org/databus#>
    SELECT DISTINCT ?artifact WHERE {{
      ?artifact databus:group <https://databus.dbpedia.org/{USER}/{group}> .
      ?artifact a databus:Artifact .
    }}
    """
    results = query_sparql(query)
    return [r["artifact"]["value"] for r in results]

def get_versions(group):
    query = f"""
    PREFIX databus: <https://dataid.dbpedia.org/databus#>
    SELECT DISTINCT ?version WHERE {{
      ?version databus:group <https://databus.dbpedia.org/{USER}/{group}> .
      ?version a databus:Version .
    }}
    """
    results = query_sparql(query)
    return [r["version"]["value"] for r in results]

def delete_resource(uri):
    print(f"Deleting: {uri}")
    response = requests.delete(uri, headers=HEADERS)
    if response.status_code in (200, 204):
        print("✅ Deleted successfully")
    else:
        print(f"❌ Failed to delete {uri} — {response.status_code}: {response.text}")

def main():
    print(f"Fetching all versions in group '{GROUP}'...")
    versions = get_versions(GROUP)
    for version_uri in versions:
        delete_resource(version_uri)

    print(f"\nFetching all artifacts in group '{GROUP}'...")
    artifacts = get_artifacts(GROUP)
    for artifact_uri in artifacts:
        delete_resource(artifact_uri)

    print(f"\nFinally, deleting group: {GROUP}")
    group_uri = f"{DATABUS_BASE}/{USER}/{GROUP}"
    delete_resource(group_uri)

if __name__ == "__main__":
    main()
