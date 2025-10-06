import glob
import json
from multiprocessing import Pool
from tqdm import tqdm

from s2ag_parser.s2orc_utils import build_s2orc
from s2ag_parser.schemas import PaperSchema

def process_line(line: str) -> dict | None:
    raw_s2orc = json.loads(line)
    try:
        s2orc = build_s2orc(raw_s2orc).model_dump()
        metadata = corpusid2metadata[s2orc["corpusid"]] 
        paper = PaperSchema(**s2orc, **metadata).model_dump()
        return paper
    except:
        print(f"Something went wrong with processing corpusid={raw_s2orc['corpusid']}")
        return None

if __name__ == "__main__":
    # Step 1: load all metadata into memory
    print(f"Loading metadata...")
    corpusid2metadata = {}
    with open("data/extracted/metadata.jsonl", "r") as f:
        for line in tqdm(f):
            x = json.loads(line)
            assert x["corpusid"] not in corpusid2metadata
            
            del x["corpusid"] # to avoid duplicating corpusid
            corpusid2metadata[x["corpusid"]] = x

    # Step 2: Process all papers and combine with metadata (do not create a separate file for s2orc because it takes so much space)
    filepaths = sorted(list(glob.glob("data/raw/s2orc/*")))
    print(f"Total number of filepaths: {len(filepaths)}")

    out_path = "data/extracted/papers.jsonl"
    print(f"Writing to {out_path}")
    with open(out_path, "w") as f_out:
        for i, filepath in enumerate(filepaths):
            print(f"Filepath {i}: {filepath}")
            with open(filepath, "r") as f, Pool(10) as p:
                for result in p.imap(process_line, tqdm(f.readlines())):
                    if result is not None:
                        print(json.dumps(result), file=f_out, flush=True)