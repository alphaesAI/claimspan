"""
Factory module for instantiating domain-specific EDI mappers dynamically.
"""

from typing import Literal
from .base import BaseMapper
from .claimsmapper import ClaimsMapper
from .membermapper import MemberMapper


class MapperFactory:
    """
    Factory class responsible for creating appropriate mapper instances based on domain type.
    """

    @staticmethod
    def get_mapper(domain_type: Literal["member", "claims"]) -> BaseMapper:
        """
        Instantiates and returns the mapper corresponding to the requested domain.

        Args:
            domain_type (Literal["member", "claims"]): The domain key for mapper selection.

        Returns:
            BaseMapper: An instance of MemberMapper or ClaimsMapper.
        """
        if domain_type == "member":
            return MemberMapper()
        elif domain_type == "claims":
            return ClaimsMapper()
        else:
            raise ValueError(f"Invalid domain type: {domain_type}")