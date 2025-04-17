"""Result Module

This module defines classes for handling scraped data with validation and signal integration.
"""

import time
from typing import Any, Dict, Optional, Union
from pydantic import BaseModel, ConfigDict, field_validator, Field as PydanticField


class Field(BaseModel):
    """Field class for storing extracted data with metadata using Pydantic.

    Attributes:
        value: The actual field value (cannot be None)
        source: Source URL where this field was extracted from
        extractor: Name of the extractor that created this field
        timestamp: When this field was created (auto-generated)
        metadata: Additional metadata about this field
    """

    value: Any
    source: Optional[str] = None
    extractor: Optional[str] = None
    timestamp: float = PydanticField(default_factory=time.time)
    metadata: Dict[str, Any] = PydanticField(default_factory=dict)

    @field_validator("value")
    def value_cannot_be_none(cls, v):
        """Validate that value is not None."""
        if v is None:
            raise ValueError("Field value cannot be None")
        return v

    @field_validator("extractor")
    def extractor_must_be_str(cls, v):
        """Validate that extractor is a string."""
        if v is not None and not isinstance(v, str):
            raise ValueError("Extractor must be a string")
        return v

    model_config = ConfigDict(
        validate_assignment=True,  # Validate assignments to fields
        validate_default=True,  # Validate default values
        extra="forbid",  # Prevent extra fields
    )


class Result:
    """Result class for storing and validating scraped data.

    Provides methods for field management, validation, and serialization.
    Uses __slots__ for memory optimization.
    """

    __slots__ = (
        "spider",
        "fields",
        "status_code",
        "url",
        "mediatype",
        "title",
        "crawl_time",
        "_errors",
        "_warnings",
    )

    def __init__(
        self,
        spider: str,
        fields: Dict[str, Union[Field, Any]],
        status_code: int = 200,
        url: Optional[str] = None,
        mediatype: Optional[str] = None,
        title: Optional[str] = None,
        crawl_time: float = 0.0,
    ):
        """Initialize Result instance.

        Args:
            spider: Name of spider that generated this result
            fields: Dictionary of field names to Field objects or raw values
            status_code: HTTP status code (default: 200)
            url: Source URL where data was extracted from
            mediatype: Content type of the source
            title: Title of the source page/document
            crawl_time: Time taken to crawl (default: current time)
        """
        self.spider = spider
        self.fields: Dict[str, Field] = {}
        self._errors = []
        self._warnings = []

        # Convert raw values to Field objects and validate
        for name, field in fields.items():
            try:
                if not isinstance(field, Field):
                    field = Field(value=field, source=url, extractor=f"{spider}.parse")
                self.fields[name] = field
            except ValueError as e:
                self._errors.append(f"Invalid field '{name}': {str(e)}")

        self.status_code = status_code
        self.url = url
        self.mediatype = mediatype
        self.title = title
        self.crawl_time = crawl_time or time.time()

    def __repr__(self) -> str:
        """String representation of Result.

        Returns:
            Formatted string showing URL and title
        """
        return f"<Result {self.url} [{self.title}]>"

    def add_field(self, name: str, field: Union[Field, Any]) -> None:
        """Add a new field to the result.

        Args:
            name: Field name
            field: Field instance or raw value

        Note:
            Raw values will be automatically converted to Field objects
        """
        try:
            if not isinstance(field, Field):
                field = Field(
                    value=field, source=self.url, extractor=f"{self.spider}.parse"
                )
            self.fields[name] = field
        except ValueError as e:
            self._errors.append(f"Invalid field '{name}': {str(e)}")

    def get_field(self, name: str) -> Optional[Field]:
        """Get field by name.

        Args:
            name: Field name to retrieve

        Returns:
            Field object if found, None otherwise
        """
        return self.fields.get(name)

    def get_value(self, name: str) -> Any:
        """Get raw field value by name.

        Args:
            name: Field name to retrieve

        Returns:
            Field value if found, None otherwise
        """
        field = self.get_field(name)
        return field.value if field else None

    def is_valid(self) -> bool:
        """Check if result contains validation errors.

        Returns:
            True if no errors, False otherwise
        """
        return len(self._errors) == 0

    def get_errors(self) -> list:
        """Get list of validation errors.

        Returns:
            List of error messages
        """
        return self._errors.copy()  # Return copy to prevent modification

    def get_warnings(self) -> list:
        """Get list of validation warnings.

        Returns:
            List of warning messages
        """
        return self._warnings.copy()  # Return copy to prevent modification

    def to_dict(self) -> dict:
        """Convert result to dictionary representation.

        Returns:
            Dictionary containing all result data including fields
        """
        return {
            "spider": self.spider,
            "fields": {
                name: field.model_dump() for name, field in self.fields.items()
            },
            "status": self.status_code,
            "url": self.url,
            "mediatype": self.mediatype,
            "title": self.title,
            "crawl_time": self.crawl_time,
            "errors": self._errors.copy(),
            "warnings": self._warnings.copy(),
        }

    def save(self) -> None:
        """Placeholder method for saving results.

        Note:
            Should be implemented by subclasses for actual persistence
        """
        pass
