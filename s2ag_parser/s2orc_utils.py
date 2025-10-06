import ast
from collections import defaultdict
import regex as re

from s2ag_parser.schemas import *

def sanitize_annotations(annotations: dict, text_len: int) -> dict:
    out = {}
    for key, _annotations in annotations.items():
        # ensure annotations is always a list
        if _annotations is None:
            out[key] = []
            continue
        
        out[key] = _annotations
        
        try:
            # literal eval all annotations for easier access later on
            _annotations_new = ast.literal_eval(_annotations)
            assert isinstance(_annotations_new, list)
            out[key] = _annotations_new
            
            if isinstance(_annotations_new, dict):
                    _annotations_new["start"] = int(_annotations_new["start"])
                    _annotations_new["end"] = int(_annotations_new["end"])
            elif isinstance(_annotations_new, list):
                for _ in _annotations_new:
                    _["start"], _["end"] = int(_["start"]), int(_["end"])
            out[key] = _annotations_new
        except:
            print(f"Unable to literal eval {key=} annotations")

        try:
            # deduplicate and keep valid annotations only, i.e. where 0 <= start < end <= len(text)
            _annotations_new = []
            seen_idxs = set()
            for ann in out[key]:
                idxs = (ann["start"], ann["end"])
                if idxs not in seen_idxs and 0 <= idxs[0] < idxs[1] <= text_len:
                    _annotations_new.append(ann)
                    seen_idxs.add(idxs)

            _annotations_new.sort(key=lambda _: _["start"])
            out[key] = _annotations_new
        except:
            print(f"Unable to deduplicate {key=} annotations")
        
        try:
            # merge overlapping annotations
            if len(out[key]) > 1:
                _annotations_new = [out[key][0]]
                for curr in out[key][1:]:
                    prev = _annotations_new[-1]
                    if curr["start"] < prev["end"]:
                        prev["end"] = max(prev["end"], curr["end"])
                        
                        for k, v in curr.get("attributes", {}).items():
                            if k not in prev:
                                prev[k] = v
                    else:
                        _annotations_new.append(curr)
                out[key] = _annotations_new
        except:
            print(f"Unable to merge overlapping {key=} annotations")
    return out

def build_bibliography(annotations: dict, raw_text: str, original2new_id: dict) -> list[BibliographyEntrySchema]:
    bibliography = []
    bibliography_annotations = sorted(
        annotations.get("bibentry", []), key = lambda x: x["start"],
    )
    for i, ann_i in enumerate(bibliography_annotations):
        try:
            corpusid = ann_i.get("attributes", {}).get("matched_paper_id")
            assert isinstance(corpusid, int)
        except:
            corpusid=None

        new_id_i = i
        original_id_i = ann_i.get("attributes", {}).get("id")
        bibliography.append(BibliographyEntrySchema(
            bibliography_id=new_id_i,
            corpusid = corpusid,
            text = raw_text[ann_i["start"]:ann_i["end"]],
            original_span = SpanSchema(**ann_i),
            original_id = original_id_i,
        ))
        original2new_id[original_id_i] = new_id_i

    return bibliography

def collect_content_annotations(annotations: dict) -> list[dict]:
    content_annotations = []
    for key in [
        "sectionheader", "paragraph", 
        "figure", "figurecaption",
        "formula",
        # "table",     # table annotations are actually also under figure annotations. ignore table annotations; they have less info!
    ]:
        for _annotation in annotations[key]:
            if key == "figure" and _annotation.get("attributes", {}).get("type") == "table":
                _annotation["key"] = "table"
            else:
                _annotation["key"] = key
            content_annotations.append(_annotation)
    content_annotations.sort(key=lambda x: x["start"])

    return content_annotations

def build_leaf_content(
        content_annotations: list[dict], 
        raw_text: str,
        original2new_id: dict,
    ) -> tuple[list, list, set]:

    def find_overlaps(_annotations: list[dict]) -> dict:
        overlaps = defaultdict(list)
        temp = [(i, x["start"], x["end"], x["key"]) for i, x in enumerate(_annotations)]
        for i, (idx1, start1, end1, key1) in enumerate(temp):
            for idx2, start2, end2, key2 in temp[i+1:]:
                if start2 < end1:
                    overlaps[idx1, key1].append((idx2, key2))
                    overlaps[idx2, key2].append((idx1, key1))
                else:
                    break
        return overlaps
    
    overlaps = find_overlaps(content_annotations)

    infographics = []
    formulas = []
    done_idxs = set()
    for i, ann_i in enumerate(content_annotations):
        content_id = (i,)     # this will be overwritten in a later step
        key_i = ann_i["key"]
        text_i = raw_text[ann_i["start"]:ann_i["end"]]
        span_i = SpanSchema(**ann_i)
        original_id_i = ann_i.get("attributes", {}).get("id")

        if key_i in ["figure", "table"]:
            overlaps_with_ann_i = overlaps[i, key_i]

            # identify the overlapping annotation that is marked as sectionheader, then extract the header
            try:
                j = [j for j, key_j in overlaps_with_ann_i if key_j == "sectionheader"][0]
                ann_j = content_annotations[j]

                header = TextSpanSchema(
                    text=raw_text[ann_j["start"]:ann_j["end"]], 
                    original_span=SpanSchema(**ann_j),
                )
                done_idxs.add(j)
            except:
                header = TextSpanSchema(text="", original_span=None)
            
            # identify the overlapping annotation that is marked as figurecaption, then extract the caption
            try:
                j = [j for j, key_j in overlaps_with_ann_i if key_j == "figurecaption"][0]
                ann_j = content_annotations[j]

                caption = TextSpanSchema(
                    text=raw_text[ann_j["start"]:ann_j["end"]], 
                    original_span=SpanSchema(**ann_j),
                )
                done_idxs.add(j)
            except:
                caption = TextSpanSchema(text="", original_span=None)

            infographics.append(InfographicSchema(
                content_id = content_id,
                content_type = key_i,
                header = header,
                caption = caption,
                text = text_i,
                original_span = span_i,
                original_id = original_id_i,
            ))

        elif key_i == "formula":
            formulas.append(FormulaSchema(
                content_id = content_id,
                content_type = key_i,
                text = text_i,
                original_span = span_i,
                original_id = original_id_i,
            ))
        
        else:
            continue

        # update record of done idxs and also id mapping (if repeated, take the first occurrence)
        done_idxs.add(i)
        try:
            assert isinstance(original_id_i, str) and original_id_i not in original2new_id
            original2new_id[original_id_i] = content_id
        except:
            pass
    
    return infographics, formulas, done_idxs

def build_reference_markers(annotations: dict, raw_text: str, original2new_id: dict) -> list[ReferenceMarkerSchema]:
    reference_markers = []
    for reference_marker_type in allowed_reference_marker_types:
        for ann in annotations[reference_marker_type]:
            
            referenced_original_id = ann.get("attributes", {}).get("ref_id")
            referenced_id = original2new_id.get(referenced_original_id)

            reference_markers.append(ReferenceMarkerSchema(
                referenced_id = referenced_id,
                reference_marker_type = reference_marker_type,
                text = raw_text[ann["start"]:ann["end"]],
                original_span = SpanSchema(**ann),
                relative_span = None,   # just for now, because we have not figured out where each reference marker belongs
            ))
    return reference_markers

def build_paragraphs(
        content_annotations: list[dict], 
        raw_text: str, 
        reference_markers: list[ReferenceMarkerSchema], 
        done_idxs: set[int],
    ) -> tuple[list[ParagraphSchema], set[int]]:
    paragraphs = []
    for i, ann_i in enumerate(content_annotations):
        if i in done_idxs or ann_i["key"] != "paragraph":
            continue
        
        # construct paragraph
        span_i = SpanSchema(**ann_i)
        paragraph = ParagraphSchema(
            content_id = (i,),
            content_type = ann_i["key"],
            text = raw_text[ann_i["start"]:ann_i["end"]],
            original_span = span_i,
            reference_markers = [],
        )

        # identify the reference markers that belong to this paragraph
        for reference_marker in reference_markers:
            span_j = reference_marker.original_span
            if (span_i.start <= span_j.start and span_j.end <= span_i.end):
                reference_marker.relative_span = SpanSchema(
                    start = span_j.start - span_i.start,
                    end = span_j.end - span_i.start,
                )
                paragraph.reference_markers.append(reference_marker)

        paragraphs.append(paragraph)

        # update record of done idxs
        done_idxs.add(i)

    # deduplicate consecutive paragraphs
    if len(paragraphs) > 1:
        deduplicated_paragraphs = [paragraphs[0]]
        for prev, curr in zip(paragraphs, paragraphs[1:]):
            if curr.text.startswith(prev.text) or \
                (curr.text == prev.text and len(curr.reference_markers) > len(prev.reference_markers)):
                deduplicated_paragraphs[-1] = curr
            else:
                deduplicated_paragraphs.append(curr)
        paragraphs = deduplicated_paragraphs

    return paragraphs, done_idxs

def build_sections(
        content_annotations: list[dict], 
        raw_text: str, 
        done_idxs: set[int],
    ) -> tuple[list[SectionSchema], set[int]]:
    sections = []
    depth2section_levels = defaultdict(set)
    for i, ann_i in enumerate(content_annotations):
        if i in done_idxs or ann_i["key"] != "sectionheader":
            continue
        
        span_i = SpanSchema(**ann_i)
        text_i = raw_text[span_i.start:span_i.end]
        if sections and sections[-1].header.text == text_i:
            done_idxs.add(i)
            continue

        # infer section id
        try:
            # take n as parsed by S2AG, if it's a nonempty string
            original_n = ann_i["attributes"]["n"]
            assert isinstance(original_n, str) and len(original_n)
            inferred_n = original_n
        except:
            try:
                # otherwise, try to infer from the header
                temp = re.search(r"^\s*([\w.]+)", text_i).group()
                assert "." in temp
                inferred_n = temp
            except:
                # if n still does not exist, just set it to an empty string
                inferred_n = ""

        # remove whitespaces and periods at ends
        inferred_n = inferred_n.strip(" .")

        # replace anything that is not alnum by periods
        inferred_n = "".join([c if c.isalnum() else "." for c in inferred_n])

        # deduplicate consecutive periods
        inferred_n = re.sub(r"\.{2,}", r".", inferred_n)

        # get inferred hierarchy based on n
        section_level = tuple(inferred_n.split("."))

        # update section records
        depth = len(section_level)
        depth2section_levels[depth].add(section_level)
        if depth > 1 and section_level[:-1] not in depth2section_levels[depth-1]:
            # insert parent section, with start/end being just the start of span_i
            parent_section_level = section_level[:-1]
            header = TextSpanSchema(
                text="", 
                original_span=SpanSchema(start=span_i.start, end=span_i.start),
            )
            sections.append(SectionSchema(
                content_id = None,
                content_type = "section",
                section_level = parent_section_level,
                header = header,
                contents = [],
            ))
            depth2section_levels[depth-1].add(parent_section_level)
        sections.append(SectionSchema(
            content_id = (i,),
            content_type = "section",
            section_level = section_level,
            header = TextSpanSchema(text=text_i, original_span=span_i),
            contents = [],   # empty for now
        ))
        done_idxs.add(i)

    return sections, done_idxs

def assign_leaf_content_to_sections(
        sections: list[SectionSchema], 
        leaf_text_contents: list[ParagraphSchema, FormulaSchema], 
        infographics: list[InfographicSchema],
    ) -> list[SectionSchema]:

    dummy_section = SectionSchema(
        content_id = (-1,),
        content_type = "section",
        section_level = ("",),
        header = TextSpanSchema(
            text="[[Dummy First Section]]", 
            original_span = SpanSchema(start=0, end=0),  # rubbish values, just for now
        ),
        contents = [],    
    )
    for leaf_content in leaf_text_contents:
        # if content ends before first section header, add to dummy section
        if not sections or leaf_content.original_span.end < sections[0].header.original_span.start:
            dummy_section.contents.append(leaf_content)
            continue

        # add content to most recent section
        parent_section = None
        for section in sections:
            if section.header.original_span.end < leaf_content.original_span.start:
                parent_section = section
            else:
                break
        if parent_section:
            parent_section.contents.append(leaf_content)

    # add infographics before the paragraph following the one that references it for the first time
    # if referencing paragraph is final paragraph of section, then add to end of section
    misc_infographics = []
    for infographic in infographics:
        inserted = False
        for section in sections:
            for i, content in enumerate(section.contents):
                if content.content_type != "paragraph":
                    continue
                    
                if infographic.content_id in [m.referenced_id for m in content.reference_markers]:
                    insert_idx = None
                    for j in range(i+1, len(section.contents)):
                        if section.contents[j].content_type == "paragraph":
                            insert_idx = j
                            break
                    
                    if insert_idx:
                        section.contents.insert(insert_idx, infographic)
                    else:
                        section.contents.append(infographic)
                        
                    inserted = True
                    break

            if inserted:
                break

        if not inserted:
            misc_infographics.append(infographic)

    # create new section for infographics with no referencing paragraph
    if misc_infographics:
        i = misc_infographics[0].original_span.start
        header = TextSpanSchema(
            text = "[[Miscellaneous Infographics]]",
            original_span = SpanSchema(start=i, end=i),
        )
        sections.append(SectionSchema(
            content_id = (len(sections),),
            content_type = "section",
            section_level = ("",), 
            header = header,
            contents = misc_infographics,
        ))

    # establish dummy section as new first section, if it has content
    if dummy_section.contents:
        # set start and end to the start of the first paragraph
        dummy_section.header.original_span.start = dummy_section.contents[0].original_span.start
        dummy_section.header.original_span.end = dummy_section.contents[0].original_span.start
        sections.insert(0, dummy_section)

    return sections

def nest_sections(sections: list[SectionSchema]) -> list[SectionSchema]:
    # nest sections based on section level information
    if any(section.section_level for section in sections):
        nested_sections = []
        # keep track of current section nesting in stack
        stack = []
        for curr in sections:
            try:
                # pop from stack until we find a parent whose n is a prefix
                while stack:
                    if curr.section_level[:-1] == stack[-1].section_level:
                        break
                    stack.pop()
            except:
                pass

            if stack:
                # current section is child of previous section in stack
                stack[-1].contents.append(curr)
            else:
                # current section is new top-level section
                nested_sections.append(curr)

            # add current section to stack
            stack.append(curr)

        sections = nested_sections

    # FUTUREWORK: nest based on IMRAD heuristics instead # FUTUREWORK: use LLM to perform nesting instead
    else:
        pass

    return sections

def reassign_content_ids(sections: list[SectionSchema]):
    old2new_content_id = {}

    def content_updater(contents, _parent_id):
        for i, content in enumerate(contents):
            new_content_id = _parent_id + (i,)
            old2new_content_id[content.content_id] = new_content_id
            content.content_id = new_content_id

            if content.content_type == "section":
                content_updater(content.contents, content.content_id)

    content_updater(sections, ())

    def referenced_content_id_updater(contents):
        for content in contents:
            if content.content_type == "paragraph":
                for marker in content.reference_markers:
                    if marker.reference_marker_type in ["figureref", "tableref"]:
                        marker.referenced_id = old2new_content_id.get(marker.referenced_id)
            elif content.content_type == "section":
                referenced_content_id_updater(content.contents)

    referenced_content_id_updater(sections)

def build_s2orc(raw_s2orc: dict) -> S2ORCSchema:
    # extract raw text of paper
    raw_text = raw_s2orc["content"]["text"] or ""

    # sanitize the annotations done by S2AG
    annotations = sanitize_annotations(raw_s2orc["content"]["annotations"].copy(), len(raw_text))

    # get bibliography, reference markers, and leaf contents
    original2new_id = {}
    bibliography = build_bibliography(annotations, raw_text, original2new_id)
    
    content_annotations = collect_content_annotations(annotations)
    infographics, formulas, done_idxs = build_leaf_content(content_annotations, raw_text, original2new_id)
    reference_markers = build_reference_markers(annotations, raw_text, original2new_id)
    paragraphs, done_idxs = build_paragraphs(content_annotations, raw_text, reference_markers, done_idxs)

    # arrange leaf contents (paragraphs and formulas) based on start position
    leaf_contents = paragraphs + formulas
    leaf_contents.sort(key = lambda x: x.original_span.start)

    # now build sections, inserting parent sections as necessary along the way
    sections, done_idxs = build_sections(content_annotations, raw_text, done_idxs)
    sections = assign_leaf_content_to_sections(sections, leaf_contents, infographics)
    sections = nest_sections(sections)

    # redefine all content_ids in a way that respects the section nesting
    reassign_content_ids(sections)

    return S2ORCSchema(
        corpusid = raw_s2orc["corpusid"],
        contents = sections,
        bibliography = bibliography,
    )