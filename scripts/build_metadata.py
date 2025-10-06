import glob
import json
from multiprocessing import Pool
from tqdm import tqdm

from s2ag_parser.schemas import MetadataSchema

def extract_abstract(line: str) -> dict:
    x = json.loads(line)
    corpusid = x["corpusid"]
    try:
        assert isinstance(corpusid, int)
    except:
        f"Encountered non-int corpusid for {x}"
    
    abstract = x["abstract"]
    if abstract is None:
        abstract = ""
    try:
        assert isinstance(abstract, str)
    except:
        f"Encountered non-str abstract for {x}"
    
    return {"corpusid": corpusid, "abstract": abstract}

def extract_main_info(line: str) -> dict:
    x = json.loads(line)
    corpusid = x["corpusid"]
    try:
        assert isinstance(corpusid, int)
    except:
        f"Encountered non-int corpusid for {x}"
    
    title = x["title"]
    if title is None:
        title = ""
    try:
        assert isinstance(title, str)
    except:
        f"Encountered non-str title for {x}"

    year = x["year"]
    try:
        assert year is None or isinstance(year, int)
    except:
        f"Encountered non-[None, int] year for {x}"

    s2fieldsofstudy = x["s2fieldsofstudy"]
    if s2fieldsofstudy is None:
        s2fieldsofstudy = []
    try:
        assert isinstance(s2fieldsofstudy, list)
    except:
        f"Encountered non-list s2fieldsofstudy for {x}"
    
    return {
        "corpusid": corpusid, 
        "title": title, 
        "year": year, 
        "s2fieldsofstudy": s2fieldsofstudy,
    }

if __name__ == "__main__":
    # Step 1: Extract abstracts and write to a file
    print(f"Extracting abstracts...")
    filepaths = sorted(list(glob.glob("data/raw/abstracts/*")))
    print(f"Total number of filepaths: {len(filepaths)}")

    abstracts_path = "data/extracted/abstracts.jsonl"
    print(f"Writing to {abstracts_path}")
    with open(abstracts_path, "w") as f_out:
        for i, filepath in enumerate(filepaths):
            print(f"Filepath {i}: {filepath}")
            with open(filepath, "r") as f, Pool(10) as p:
                for result in p.imap(extract_abstract, tqdm(f.readlines())):
                    if result is not None:
                        print(json.dumps(result), file=f_out, flush=True)
    print()

    # Step 2: Extract main info and keep in memory
    print(f"Extracting main info...")
    filepaths = sorted(list(glob.glob("data/raw/papers/*")))
    print(f"Total number of filepaths: {len(filepaths)}")

    corpusid2main_info = {}
    for i, filepath in enumerate(filepaths):
        print(f"Filepath {i}: {filepath}")
        with open(filepath, "r") as f, Pool(20) as p:
            for result in p.imap(extract_main_info, tqdm(f.readlines())):
                if result is not None:
                    corpusid2main_info[result["corpusid"]] = result
    print()
    
    # Step 3: Read in abstracts line by line, find corresponding entry in memory, and write to final file
    metadata_path = "data/extracted/metadata.jsonl"
    corpusid2line_num = {}
    line_num = 0
    print(f"Writing to {metadata_path}")
    with open(abstracts_path, "r") as f_in, open(metadata_path, "w") as f_out:
        for line in tqdm(f_in):
            x = json.loads(line)

            corpusid = x["corpusid"]
            if corpusid not in corpusid2main_info:
                print(f"Could not find entry with {corpusid=}")
            else:
                temp = dict(**corpusid2main_info[corpusid])
                temp["abstract"] = x["abstract"]
                metadata = MetadataSchema(**temp).model_dump()
                print(json.dumps(metadata), file=f_out, flush=True)

                corpusid2line_num[corpusid] = line_num
                line_num += 1

    # Step 4: Write out corpusid to line number mapping
    corpusid2metadata_line_num = "data/extracted/corpusid2metadata_line_num.json"
    print(f"Writing to {corpusid2metadata_line_num}")
    with open(corpusid2metadata_line_num, "w") as f_out:
        json.dump(corpusid2line_num, f_out)