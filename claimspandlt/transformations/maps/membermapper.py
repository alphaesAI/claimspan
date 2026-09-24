"""
Member domain mapping implementation for EDI 834 data.
"""

from typing import Union, List, Dict
from .base import BaseMapper
from .config import MEMBERMAPPINGS


class MemberMapper(BaseMapper):
    """
    Maps EDI 834 structured JSON into flattened CSV-friendly member records.
    """

    def __init__(self):
        """
        Initializes the MemberMapper with EDI 834 mapping expressions.
        """
        from pyedi import SchemaMapper
        self.mapper = SchemaMapper(MEMBERMAPPINGS)

    def map(self, structured_json: Union[Dict, List[Dict]]) -> Union[Dict, List[Dict]]:
        """
        Maps single or multiple EDI 834 structured JSON records.

        Args:
            structured_json (Union[Dict, List[Dict]]): Parsed EDI 834 JSON payload.

        Returns:
            Union[Dict, List[Dict]]: Mapped member output data structure.
        """
        if isinstance(structured_json, list):
            return [self.mapper.map(record) for record in structured_json]
        return self.mapper.map(structured_json)

    def map_member(self, structured_json: Union[Dict, List[Dict]]) -> Union[Dict, List[Dict]]:
        """
        Alias method for mapping EDI 834 member records.

        Args:
            structured_json (Union[Dict, List[Dict]]): Parsed EDI 834 JSON payload.

        Returns:
            Union[Dict, List[Dict]]: Mapped member output data structure.
        """
        return self.map(structured_json)