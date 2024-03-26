from dataclasses import dataclass, field
import datetime

@dataclass
class Base:
    """
    Base class providing metadata fields for tracking the creation, 
    modification, and revision history of records.
    """
    
    _createdat: datetime.datetime = field(
        default_factory=datetime.datetime.now, 
        metadata={
            "Field Definition": "Metadata field describing when this record was created in the target data store."
        }
    )
    
    _updatedat: datetime.datetime = field(
        default_factory=datetime.datetime.now, 
        metadata={
            "Field Definition": "Metadata field describing when this record was updated in the target data store."
        }
    )
    
    _rev: int = field(
        default=1, 
        metadata={
            "Field Definition": "Metadata field describing how many times this record has been revised. Starting counter is 1."
        }
    )

    def update_timestamps(self):
        """
        Updates the modification timestamp and increments the revision counter.
        """
        self._updatedat = datetime.datetime.now()
        self._rev += 1
