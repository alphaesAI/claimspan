"""
Claims domain mapping implementation for EDI 837 data.
"""

from typing import Union, List, Dict
from .base import BaseMapper
from .config import CLAIMSMAPPINGS


class ClaimsMapper(BaseMapper):
    """
    Maps EDI 837 structured JSON into flattened CSV-friendly claim records.
    """

    def __init__(self):
        """
        Initializes the ClaimsMapper with EDI 837 mapping expressions.
        """
        from pyedi import SchemaMapper
        self.mapper = SchemaMapper(CLAIMSMAPPINGS)

    def map(self, structured_json: Union[Dict, List[Dict]]) -> Union[Dict, List[Dict]]:
        """
        Maps single or multiple EDI 837 structured JSON records.

        Args:
            structured_json (Union[Dict, List[Dict]]): Parsed EDI 837 JSON payload.

        Returns:
            Union[Dict, List[Dict]]: Mapped claims output data structure.
        """
        if isinstance(structured_json, list):
            return [self.mapper.map(record) for record in structured_json]
        return self.mapper.map(structured_json)

    def map_claims(self, structured_json: Union[Dict, List[Dict]]) -> Union[Dict, List[Dict]]:
        """
        Alias method for mapping EDI 837 claims records.

        Args:
            structured_json (Union[Dict, List[Dict]]): Parsed EDI 837 JSON payload.

        Returns:
            Union[Dict, List[Dict]]: Mapped claims output data structure.
        """
        return self.map(structured_json)