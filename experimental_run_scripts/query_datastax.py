import json
import argparse
from langchain_astradb import AstraDBVectorStore
from astrapy.info import CollectionVectorServiceOptions

def query_similarity(query, api_key_json, k=10):

    # Load API key from JSON file
    datastax_token = json.load(open(api_key_json))
    ASTRA_DB_DATABASE_ID = datastax_token['ASTRA_DB_DATABASE_ID']
    ASTRA_DB_APPLICATION_TOKEN = datastax_token['ASTRA_DB_APPLICATION_TOKEN']
    ASTRA_DB_API_ENDPOINT = datastax_token["ASTRA_DB_API_ENDPOINT"]
    ASTRA_DB_KEYSPACE = datastax_token["ASTRA_DB_KEYSPACE"]

    # Set up CollectionVectorServiceOptions with NVIDIA model
    collection_vector_service_options = CollectionVectorServiceOptions(
        provider="nvidia",
        model_name="NV-Embed-QA"
    )

    # Initialize the graph store
    graph_store = AstraDBVectorStore(
        collection_name="wikidata",
        collection_vector_service_options=collection_vector_service_options,
        token=ASTRA_DB_APPLICATION_TOKEN,
        api_endpoint=ASTRA_DB_API_ENDPOINT,
        namespace=ASTRA_DB_KEYSPACE,
    )

    # Perform similarity search using the user-provided query and k value
    results = graph_store.similarity_search(query, k=k)

    # Print the results
    for result in results:
        print()
        print(f"{result.metadata['QID']}:\n{result.page_content}")

    return results

if __name__ == "__main__":
    # Set up argument parsing
    parser = argparse.ArgumentParser(description="Search using AstraDBVectorStore and NVIDIA embeddings")

    # Arguments
    parser.add_argument('--api_key_json', required=True, help='Path to the API key JSON file')
    parser.add_argument('--query', required=True, help='User query for similarity search')
    parser.add_argument('--k', type=int, default=10, help='Number of top results to return')

    # Parse arguments
    args = parser.parse_args()

    results = query_similarity(args.query, args.api_key_json, args.k)
