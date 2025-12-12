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
        metadata = {"title": None, "year": None}
        paper = PaperSchema(**s2orc, **metadata).model_dump()
        return paper
    except:
        print(f"Something went wrong with processing corpusid={raw_s2orc['corpusid']}")
        return None

if __name__ == "__main__":
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