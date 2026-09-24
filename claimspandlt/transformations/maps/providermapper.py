"""
Provider domain mapping implementation for EDI 274 data.
"""

from typing import Union, List, Dict
from .base import BaseMapper
from .config import PROVIDERMAPPINGS


class ProviderMapper(BaseMapper):
    """
    Maps EDI 274 structured JSON into flattened provider hierarchy records.
    """

    def __init__(self):
        """
        Initializes the ProviderMapper with EDI 274 mapping expressions.
        """
        from pyedi import SchemaMapper
        self.mapper = SchemaMapper(PROVIDERMAPPINGS)

    def map(self, structured_json: Union[Dict, List[Dict]]) -> Union[Dict, List[Dict]]:
        """
        Maps single or multiple EDI 274 structured JSON records.

        Args:
            structured_json (Union[Dict, List[Dict]]): Parsed EDI 274 JSON payload.

        Returns:
            Union[Dict, List[Dict]]: Mapped provider output data structure.
        """
        if isinstance(structured_json, list):
            return [self.mapper.map(record) for record in structured_json]
        return self.mapper.map(structured_json)

    def map_provider(self, structured_json: Union[Dict, List[Dict]]) -> Union[Dict, List[Dict]]:
        """
        Alias method for mapping EDI 274 provider records.

        Args:
            structured_json (Union[Dict, List[Dict]]): Parsed EDI 274 JSON payload.

        Returns:
            Union[Dict, List[Dict]]: Mapped provider output data structure.
        """
        return self.map(structured_json)
