def is_paper(x: dict) -> bool:
    return all(key in x for key in ["corpusid", "contents", "bibliography"])

def is_content(x: dict) -> bool:
    return all(key in x for key in ["content_id", "content_type"])
    
def is_bibliography_entry(x: dict) -> bool:
    return "bibliography_id" in x

def is_reference_marker(x: dict) -> bool:
    return "referenced_id" in x
    
def is_section(x: dict) -> bool:
    return is_content(x) and get_content_type(x) == "section"
    
def is_paragraph(x: dict) -> bool:
    return is_content(x) and get_content_type(x) == "paragraph"
    
def is_figure(x: dict) -> bool:
    return is_content(x) and get_content_type(x) == "figure"
    
def is_table(x: dict) -> bool:
    return is_content(x) and get_content_type(x) == "table"
    
def is_formula(x: dict) -> bool:
    return is_content(x) and get_content_type(x) == "formula"

## Getters (non-recursive)

def get_corpusid(x: dict) -> int | None:
    return x["corpusid"]

def get_content_id(x: dict) -> list[int]:
    return x["content_id"]

def get_content_type(x: dict) -> str:
    return x["content_type"]

def get_contents(x: dict) -> list[dict]:
    return x["contents"]

def get_bibliography(x: dict) -> dict:
    return x["bibliography"]

def get_reference_markers(x: dict) -> list[dict]:
    return x["reference_markers"]

def get_text(x: dict) -> str:
    return x["text"]

def get_sections(x: dict) -> list[dict]:
    return [content for content in get_contents(x) if is_section(content)]

def get_paragraphs(x: dict) -> list[dict]:
    return [content for content in get_contents(x) if is_paragraph(content)]

def get_content(x: dict, content_id: list[int]) -> dict:
    assert has_contents(x), f"Input x must have 'contents' key"

    if is_paper(x):
        start_idx = 0
    else:
        x_content_id = get_content_id(x)
        start_idx = len(x_content_id)
        assert content_id[:start_idx] == x_content_id, f"{content_id} cannot be child content of {x_content_id}"
    
    try:
        curr = x
        for level in content_id[start_idx:]:
            curr = get_contents(curr)[level]
        return curr
    except:
        raise ValueError(f"Content with content_id={content_id} not found")

## Getters (recursive)

def has_contents(x: dict) -> bool:
    return "contents" in x

def get_contents_flat(x: dict) -> list[dict]:
    contents_flat = [x] if is_content(x) else []
    if has_contents(x):
        for content in get_contents(x):
            contents_flat += get_contents_flat(content)
    return contents_flat

def get_sections_flat(x: dict) -> list[dict]:
    sections_flat = [x] if is_section(x) else []
    if has_contents(x):
        for content in get_contents(x):
            sections_flat += get_sections_flat(content)
    return sections_flat

def get_paragraphs_flat(x: dict) -> list[dict]:
    paragrahs_flat = [x] if is_paragraph(x) else []
    if has_contents(x):
        for content in get_contents(x):
            paragrahs_flat += get_paragraphs_flat(content)
    return paragrahs_flat

## General utils
def strip_whitespace(x: str, delimiter: str) -> str:
    return delimiter.join(x.split())