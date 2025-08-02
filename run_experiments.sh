for i in $(seq 0 112); do
  sudo docker compose run --build \
    -e COLLECTION_NAME="v2_09_2025" \
    -e QUERY_LANGUAGE="en" \
    -e DB_LANGUAGE="en" \
    -e API_KEY="datastax_wikidata.json" \
    -e NUM_PROCESSES=4 \
    -e CHUNK_NUM="$i" \
    create_prototype
done
