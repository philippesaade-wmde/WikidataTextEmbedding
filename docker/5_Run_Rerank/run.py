from tqdm import tqdm
import pandas as pd
import os
import pickle

from src.wikidataLangDB import create_wikidatalang_db
from src.wikidataEmbed import WikidataTextifier
from src.JinaAI import JinaAIReranker


MODEL = os.getenv("MODEL", "jina")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", 100))
RETRIEVAL_FILENAME = os.getenv("RETRIEVAL_FILENAME")
LANGUAGE = os.getenv("LANGUAGE", 'en')
QUERY_COL = os.getenv("QUERY_COL")
RESTART = os.getenv("RESTART", "false").lower() == "true"

textifier = WikidataTextifier(language=LANGUAGE)
reranker = JinaAIReranker()


pkl_fpath = f"../data/Evaluation Data/{RETRIEVAL_FILENAME}.pkl"
with open(pkl_fpath, "rb") as pkl_file:
    eval_data = pickle.load(pkl_file)

WikidataLang = create_wikidatalang_db(db_filname=f"sqlite_{LANGUAGE}wiki.db")

# Rerank the QIDs
def rerank_qids(query, qids, reranker, textifier):
    entities = [WikidataLang.get_entity(qid) for qid in qids]
    texts = [textifier.entity_to_text(entity) for entity in entities]
    scores = reranker.rank(query, texts)

    score_zip = zip(scores, qids)
    score_zip = sorted(score_zip, key=lambda x: -x[0])
    return [x[1] for x in score_zip]


if __name__ == "__main__":
    with tqdm(total=len(eval_data), disable=False) as progressbar:
        if 'Reranked QIDs' not in eval_data:
            eval_data['Reranked QIDs'] = None

        row_to_process = pd.isna(eval_data['Reranked QIDs'])
        progressbar.update((~row_to_process).sum())
        for i in range(0, row_to_process.sum()):
            row = eval_data[row_to_process].iloc[i]

            # Rerank the QIDs
            ranked_qids = rerank_qids(
                row[QUERY_COL],
                row['Retrieval QIDs'],
                reranker,
                textifier
            )

            eval_data.loc[[row.index], 'Reranked QIDs'] = pd.Series(
                ranked_qids
            ).values

            # TODO: create new function to update tqdm progressbar
            # tqdm is not working in docker compose. This is the alternative
            progressbar.update(1)
            tqdm.write(
                progressbar.format_meter(
                    progressbar.n,
                    progressbar.total,
                    progressbar.format_dict["elapsed"]
                )
            )
            if progressbar.n % 100 == 0:
                pkl_fpath = f"../data/Evaluation Data/{RETRIEVAL_FILENAME}.pkl"
                with open(pkl_fpath, "wb") as pkl_file:
                    pickle.dump(eval_data, pkl_file, "wb")

        # TODO: Why is this definition and open twice at the end?
        pkl_fpath = f"../data/Evaluation Data/{RETRIEVAL_FILENAME}.pkl"
        with open(pkl_fpath, "wb") as pkl_file:
            pickle.dump(eval_data, pkl_file, "wb")
