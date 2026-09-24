import os
import json

class EDIProcessor:
    """
    Handles reading and parsing individual EDI files into structured JSON.
    """
    def __init__(self):
        from pyedi import X12Parser, StructuredFormatter
        self.parser = X12Parser()
        self.formatter = StructuredFormatter()

    def __call__(self, file_path: str) -> dict:
        """
        Callable interface to parse a single EDI file from a file path.
        """
        if not file_path:
            raise ValueError("File path cannot be empty.")
            
        # Normalize legacy DBFS paths to OS paths if present
        if file_path.startswith("dbfs:/"):
            file_path = file_path.replace("dbfs:/", "/dbfs/", 1)
            
        if os.path.isfile(file_path):
            return self.parse(file_path)
        else:
            raise FileNotFoundError(f"File path not found or is a directory: {file_path}")

    def parse(self, file_path: str) -> dict:
        """
        Reads raw EDI text from a single file path and formats it into structured JSON.
        """
        with open(file_path, "r", encoding="utf-8") as file:
            edi_data = file.read()

        generic_json = self.parser.parse(edi_data)
        structured_json = self.formatter.format(
            generic_json,
            include_technical=True
        )
        
        return structured_json