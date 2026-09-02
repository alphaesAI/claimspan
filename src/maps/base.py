"""
Base abstraction for EDI schema mappers.
"""

from abc import ABC, abstractmethod
from typing import Union, List, Dict


class BaseMapper(ABC):
    """
    Abstract Base Class defining the contract for all domain-specific EDI mappers.
    """

    @abstractmethod
    def map(self, structured_json: Union[Dict, List[Dict]]) -> Union[Dict, List[Dict]]:
        """
        Transforms parsed EDI structured JSON into domain-mapped flat structures.

        Args:
            structured_json (Union[Dict, List[Dict]]): Parsed EDI JSON structure or list of records.

        Returns:
            Union[Dict, List[Dict]]: Transformed schema mapping output.
        """
        pass