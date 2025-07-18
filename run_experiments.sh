# docker compose run --build \
#     -e COLLECTION_NAME="wikidatav11_v3_sorted_512dim" \
#     -e OFFSET=0 \
#     add_wikidata_to_db

# docker compose run --build \
#     -e EVALUATION_PATH="Mintaka/processed_dataframe.pkl" \
#     -e QUERY_COL="Question" \
#     -e PREFIX="_512dim" \
#     -e COLLECTION_NAME="wikidatav11_v3_sorted_512dim" \
#     -e QUERY_LANGUAGE="en" \
#     -e DB_LANGUAGE="en" \
#     -e API_KEY="datastax_wikidata2.json" \
#     run_retrieval

docker compose run --build \
    -e EVALUATION_PATH="LC_QuAD/processed_dataframe.pkl" \
    -e QUERY_COL="Question" \
    -e PREFIX="_propertytest" \
    -e COLLECTION_NAME="wikidata_prototype" \
    -e QUERY_LANGUAGE="en" \
    -e DB_LANGUAGE="en" \
    -e API_KEY="datastax_wikidata.json" \
    run_retrieval

# docker compose run --build \
#     -e EVALUATION_PATH="REDFM/processed_dataframe.pkl" \
#     -e QUERY_COL="Sentence" \
#     -e PREFIX="_512dim" \
#     -e COLLECTION_NAME="wikidatav11_v3_sorted_512dim" \
#     -e QUERY_LANGUAGE="en" \
#     -e DB_LANGUAGE="en" \
#     -e API_KEY="datastax_wikidata2.json" \
#     run_retrieval

# docker compose run --build \
#     -e EVALUATION_PATH="REDFM/processed_dataframe.pkl" \
#     -e QUERY_COL="Sentence no entity" \
#     -e PREFIX="_512dim_noentity" \
#     -e COLLECTION_NAME="wikidatav11_v3_sorted_512dim" \
#     -e QUERY_LANGUAGE="en" \
#     -e DB_LANGUAGE="en" \
#     -e API_KEY="datastax_wikidata2.json" \
#     run_retrieval

docker compose run --build \
    -e EVALUATION_PATH="RuBQ/processed_dataframe.pkl" \
    -e QUERY_COL="Question" \
    -e PREFIX="_propertytest" \
    -e COLLECTION_NAME="wikidata_prototype" \
    -e QUERY_LANGUAGE="en" \
    -e DB_LANGUAGE="en" \
    -e API_KEY="datastax_wikidata.json" \
    run_retrieval

# docker compose run --build \
#     -e EVALUATION_PATH="Wikidata-Disamb/processed_dataframe.pkl" \
#     -e QUERY_COL="Sentence" \
#     -e COMPARATIVE="true" \
#     -e COMPARATIVE_COLS="Correct QID,Wrong QID" \
#     -e COLLECTION_NAME="wikidatav11_v3_sorted_512dim" \
#     -e PREFIX="_512dim" \
#     -e QUERY_LANGUAGE="en" \
#     -e DB_LANGUAGE="en" \
#     -e API_KEY="datastax_wikidata2.json" \
#     run_retrieval
