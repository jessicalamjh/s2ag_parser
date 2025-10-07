import glob
import json
from multiprocessing import Pool
import os
from tqdm import tqdm

from s2ag_parser.schemas import MetadataSchema
from s2ag_parser.datautils import strip_whitespace

def extract_abstract(line: str) -> dict | None:
    x = json.loads(line)
    corpusid = x["corpusid"]
    try:
        assert isinstance(corpusid, int)
    except:
        f"Encountered non-int corpusid for {x}"
        return None
    
    abstract = x["abstract"]
    if abstract is None:
        abstract = ""
    try:
        assert isinstance(abstract, str)
        abstract = strip_whitespace(abstract, " ")
    except:
        f"Encountered non-str abstract for {x}"
    
    return {"corpusid": corpusid, "abstract": abstract}

def extract_metadata(line: str) -> dict | None:
    x = json.loads(line)
    corpusid = x["corpusid"]
    try:
        assert isinstance(corpusid, int)
    except:
        f"Encountered non-int corpusid for {x}"
        return None
    
    title = x["title"]
    if title is None:
        title = ""
    try:
        assert isinstance(title, str)
        title = strip_whitespace(title, " ")
    except:
        f"Encountered non-str title for {x}"

    year = x["year"]
    try:
        assert year is None or isinstance(year, int)
    except:
        f"Encountered non-[None, int] year for {x}"
    
    return MetadataSchema(
        corpusid=corpusid, 
        title=title, 
        year=year, 
        abstract="",   # hardcoded for now only
    ).model_dump()

if __name__ == "__main__":
    # Step 1: Extract abstracts and write to a file
    print(f"Extracting abstracts...")
    filepaths = sorted(list(glob.glob("data/raw/abstracts/*")))
    print(f"Total number of filepaths: {len(filepaths)}")

    abstracts_path = "data/extracted/abstracts.jsonl"
    if os.path.exists(abstracts_path):
        print(f"Abstracts already exist at {abstracts_path}")
    else:
        print(f"Writing to {abstracts_path}")
        with open(abstracts_path, "w") as f_out:
            for i, filepath in enumerate(filepaths):
                print(f"Filepath {i}: {filepath}")
                with open(filepath, "r") as f, Pool(10) as p:
                    for result in p.imap(extract_abstract, tqdm(f.readlines())):
                        if result is not None:
                            print(json.dumps(result), file=f_out, flush=True)
    print()

    # # Step 2: Creating abstracts index
    # abstracts_line_num2corpusid_path = "data/extracted/abstracts_line_num2corpusid.json"
    # if os.path.exists(abstracts_line_num2corpusid_path):
    #     print(f"Abstracts index already exists at {abstracts_line_num2corpusid_path}")
    #     with open(abstracts_line_num2corpusid_path, "r") as f:
    #         abstracts_line_num2corpusid = json.load(f)
    # else:
    #     print(f"Constructing abstracts index based on {abstracts_path}...")
    #     abstracts_line_num2corpusid = []
    #     with open(abstracts_path, "r") as f_abstracts:
    #         for line in tqdm(f_abstracts):
    #             x = json.loads(line)
    #             abstracts_line_num2corpusid.append(x["corpusid"])
    #     print(f"Writing to {abstracts_line_num2corpusid_path}")
    #     with open(abstracts_line_num2corpusid_path, "w") as f_out:
    #         json.dump(abstracts_line_num2corpusid, f_out)
    # print()        

    # Step 3: Creating metadata items (hardcode abstract to empty string for now)
    print(f"Building metadata...")
    filepaths = sorted(list(glob.glob("data/raw/papers/*")))
    print(f"Total number of filepaths: {len(filepaths)}")

    metadata_noabstract_path = "data/extracted/metadata_noabstract.jsonl"
    print(f"Writing to {metadata_noabstract_path}...")
    f_out = open(metadata_noabstract_path, "w")
    for i, filepath in enumerate(filepaths):
        print(f"Filepath {i}: {filepath}")
        with open(filepath, "r") as f, Pool(10) as p:
            for result in p.imap(extract_metadata, tqdm(f.readlines())):
                if result is not None:
                    print(json.dumps(result), file=f_out)
    f_out.close()
    print()
    
    # # Step 3: Read in abstracts line by line, find corresponding entry in memory, and write to final file
    # metadata_path = "data/extracted/metadata.jsonl"
    # corpusid2line_num = {}
    # line_num = 0
    # print(f"Writing to {metadata_path}")
    # with open(abstracts_path, "r") as f_in, open(metadata_path, "w") as f_out:
    #     for line in tqdm(f_in):
    #         x = json.loads(line)

    #         corpusid = x["corpusid"]
    #         if corpusid not in corpusid2main_info:
    #             print(f"Could not find entry with {corpusid=}")
    #         else:
    #             temp = dict(**corpusid2main_info[corpusid])
    #             temp["abstract"] = x["abstract"]
    #             metadata = MetadataSchema(**temp).model_dump()
    #             print(json.dumps(metadata), file=f_out, flush=True)

    #             corpusid2line_num[corpusid] = line_num
    #             line_num += 1

    # # Step 4: Write out corpusid to line number mapping
    # corpusid2metadata_line_num = "data/extracted/corpusid2metadata_line_num.json"
    # print(f"Writing to {corpusid2metadata_line_num}")
    # with open(corpusid2metadata_line_num, "w") as f_out:
    #     json.dump(corpusid2line_num, f_out)