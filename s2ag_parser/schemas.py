from pydantic import BaseModel, Field, field_validator
from typing import Annotated, Literal, Union

allowed_content_types = [
    "section", "paragraph", "figure", "table", "formula", 
]
allowed_reference_marker_types = [
    "bibref", 
    "figureref", 
    "tableref", 
    # "formularef", # this does not exist in the current S2AG
]

class SpanSchema(BaseModel):
    start: int
    end: int

class TextSpanSchema(BaseModel):
    text: str

    original_span: SpanSchema | None
    
class BibliographyEntrySchema(TextSpanSchema):
    bibliography_id: int | None
    corpusid: int | None

class ReferenceMarkerSchema(TextSpanSchema):
    referenced_id: int | tuple[int, ...] | None
    reference_marker_type: str
    
    relative_span: SpanSchema | None

    @field_validator("reference_marker_type")
    def validate_ref_type(cls, reference_marker_type):
        assert reference_marker_type in allowed_reference_marker_types
        return reference_marker_type
    
class ContentSchema(BaseModel):
    content_id: tuple[int, ...] | None  # examples: (1, 2) < (1, 2) < (5,) < (10, 2)
    content_type: str

    @field_validator("content_type")
    def validate_content_type(cls, v):
        assert v in allowed_content_types
        return v
    
class LeafContentSchema(ContentSchema, TextSpanSchema):
    pass

class ParagraphSchema(LeafContentSchema):
    content_type: Literal["paragraph"]
    reference_markers: list[ReferenceMarkerSchema] = Field(default_factory=list)
    
class FormulaSchema(LeafContentSchema):
    content_type: Literal["formula"]
    
class InfographicSchema(LeafContentSchema):
    content_type: Literal["figure", "table"]
    header: TextSpanSchema
    caption: TextSpanSchema
    
content_union = Annotated[
    Union[
        ParagraphSchema,
        FormulaSchema,
        InfographicSchema,
        "SectionSchema",  # forward ref
    ],
    Field(discriminator="content_type"),
]
class SectionSchema(ContentSchema):
    content_type: Literal["section"]
    section_level: tuple[str, ...]
    header: TextSpanSchema
    
    contents: list[content_union] = Field(default_factory=list)
    
    @field_validator("section_level")
    def validate_section_level(cls, section_level):
        assert all(not part or part.isalnum() for part in section_level)
        return section_level
    
class BaseSchema(BaseModel):
    corpusid: int

class S2ORCSchema(BaseSchema):
    contents: list[SectionSchema] = Field(default_factory=list)
    bibliography: list[BibliographyEntrySchema] = Field(default_factory=list)

class MetadataSchema(BaseSchema):
    title: str | None
    year: int | None

class PaperSchema(MetadataSchema, S2ORCSchema):
    pass