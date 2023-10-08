"""Loader that loads PDF files."""
import logging
import os
from abc import ABC
from typing import List
import secrets
import string

from langchain.docstore.document import Document
from langchain.document_loaders.base import BaseLoader

logger = logging.getLogger(__file__)

class BaseSignalPDFLoader(BaseLoader, ABC):
    """Base loader class for PDF files.
    """
    def generate_random_filename(self, len):
        characters = string.ascii_letters + string.digits
        random_string = ''.join(secrets.choice(characters) for _ in range(len))
        return random_string

    def __init__(self, s3_client, s3_bucket, s3_key: str, document_id:str, temp_dir: str, filename: str):
        """Initialize S3 assets."""
        self.s3_client = s3_client
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key    
        self.document_id = document_id    
        self.filename = filename
        self.temp_dir = temp_dir

        self.temp_file = f"{temp_dir}\{self.generate_random_filename(10)}"
        s3_client.download_file(self.s3_bucket, self.s3_key, self.temp_file)       
    
    def __del__(self) -> None:
        os.remove(self.temp_file)

class SignalPyPDFLoader(BaseSignalPDFLoader):
    """Loads a PDF with pypdf and chunks at character level.

    Loader also stores page numbers in metadatas.
    """

    def __init__(self, s3_client, s3_bucket, s3_key: str, document_id:str, temp_dir: str, filename: str):
        """Initialize with file path."""
        try:
            import pypdf  # noqa:F401
        except ImportError:
            raise ValueError(
                "pypdf package not found, please install it with " "`pip install pypdf`"
            )
        
        try:
            import boto3
        except ImportError:
            raise ValueError(
                "Could not import boto3 python package. "
                "Please install it with `pip install boto3`."
            )        
        super().__init__(s3_client, s3_bucket, s3_key, document_id, temp_dir, filename)

    def load(self) -> List[Document]:
        """Load given path as pages."""
        import pypdf

        with open(self.temp_file, "rb") as pdf_file_obj:
            pdf_reader = pypdf.PdfReader(pdf_file_obj)
            return [
                Document(
                    page_content=page.extract_text(),
                    metadata={"document_id": self.document_id, "source": self.filename, "page": i},
                )
                for i, page in enumerate(pdf_reader.pages)
            ]