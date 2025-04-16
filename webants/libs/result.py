"""Result Module

This module defines classes for handling scraped data with validation and signal integration.
"""

import time
from typing import Any, Dict, Optional
from dataclasses import dataclass, field


@dataclass
class Field:
    """Field class for storing extracted data with metadata."""
    
    value: Any
    source: Optional[str] = None
    extractor: Optional[str] = None
    timestamp: float = field(default_factory=time.time)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        """Validate field after initialization."""
        if self.value is None:
            raise ValueError("Field value cannot be None")
            
    def to_dict(self) -> dict:
        """Convert field to dictionary."""
        return {
            "value": self.value,
            "source": self.source,
            "extractor": self.extractor,
            "timestamp": self.timestamp,
            "metadata": self.metadata
        }


class Result:
    """Result class for storing and validating scraped data."""
    
    __slots__ = (
        "spider",
        "fields",
        "status",
        "url",
        "mediatype",
        "title",
        "crawl_time",
        "_errors",
        "_warnings"
    )

    def __init__(
        self,
        spider: str,
        fields: Dict[str, Field],
        status: int = 200,
        url: Optional[str] = None,
        mediatype: Optional[str] = None,
        title: Optional[str] = None,
        crawl_time: float = 0.0,
    ):
        """Initialize Result instance.
        
        Args:
            spider: Name of spider that generated this result
            fields: Dictionary of extracted fields
            status: HTTP status code
            url: Source URL
            mediatype: Content type
            title: Page title
            crawl_time: Time taken to crawl
        """
        self.spider = spider
        self.fields = {}
        self._errors = []
        self._warnings = []
        
        # Validate and set fields
        for name, field in fields.items():
            try:
                if not isinstance(field, Field):
                    field = Field(
                        value=field,
                        source=url,
                        extractor=f"{spider}.parse"
                    )
                self.fields[name] = field
            except ValueError as e:
                self._errors.append(f"Invalid field '{name}': {str(e)}")
                
        self.status = status
        self.url = url
        self.mediatype = mediatype
        self.title = title
        self.crawl_time = crawl_time or time.time()

    def __repr__(self) -> str:
        """String representation of Result."""
        return f"<Result {self.url} [{self.title}]>"

    def add_field(self, name: str, field: Field) -> None:
        """Add a new field to the result.
        
        Args:
            name: Field name
            field: Field instance or value
        """
        try:
            if not isinstance(field, Field):
                field = Field(
                    value=field,
                    source=self.url,
                    extractor=f"{self.spider}.parse"
                )
            self.fields[name] = field
        except ValueError as e:
            self._errors.append(f"Invalid field '{name}': {str(e)}")

    def get_field(self, name: str) -> Optional[Field]:
        """Get field by name.
        
        Args:
            name: Field name
            
        Returns:
            Field instance or None if not found
        """
        return self.fields.get(name)

    def get_value(self, name: str) -> Any:
        """Get field value by name.
        
        Args:
            name: Field name
            
        Returns:
            Field value or None if not found
        """
        field = self.get_field(name)
        return field.value if field else None

    def is_valid(self) -> bool:
        """Check if result is valid.
        
        Returns:
            True if no errors, False otherwise
        """
        return len(self._errors) == 0

    def get_errors(self) -> list:
        """Get validation errors.
        
        Returns:
            List of error messages
        """
        return self._errors

    def get_warnings(self) -> list:
        """Get validation warnings.
        
        Returns:
            List of warning messages
        """
        return self._warnings

    def to_dict(self) -> dict:
        """Convert result to dictionary.
        
        Returns:
            Dictionary representation of result
        """
        return {
            "spider": self.spider,
            "fields": {
                name: field.to_dict()
                for name, field in self.fields.items()
            },
            "status": self.status,
            "url": self.url,
            "mediatype": self.mediatype,
            "title": self.title,
            "crawl_time": self.crawl_time,
            "errors": self._errors,
            "warnings": self._warnings
        }

    def save(self) -> None:
        """Save result to storage.
        
        This is a placeholder method that should be implemented by subclasses
        to persist results to a database, file, etc.
        """
        pass
