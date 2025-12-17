"""
Document parser service for extracting text from PDF and Word documents
"""

import io
from typing import Union
import PyPDF2
from docx import Document
import olefile
import re


class DocumentParser:
    """Service for parsing various document formats"""
    
    async def parse_document(
        self,
        file_content: bytes,
        filename: str
    ) -> str:
        """
        Parse document and extract complete text content
        
        Args:
            file_content: File content as bytes
            filename: Original filename
        
        Returns:
            Complete extracted text content (no truncation)
        """
        try:
            if filename.lower().endswith('.pdf'):
                return await self._parse_pdf(file_content)
            elif filename.lower().endswith('.docx'):
                return await self._parse_docx(file_content)
            elif filename.lower().endswith('.doc'):
                return await self._parse_doc_legacy(file_content)
            else:
                raise ValueError(f"Unsupported file format: {filename}")
        
        except Exception as e:
            raise Exception(f"Failed to parse document {filename}: {str(e)}")
    
    async def _parse_pdf(self, file_content: bytes) -> str:
        """
        Parse PDF document and extract all text
        
        Args:
            file_content: PDF file content as bytes
        
        Returns:
            Complete extracted text from all pages
        """
        try:
            pdf_file = io.BytesIO(file_content)
            pdf_reader = PyPDF2.PdfReader(pdf_file)
            
            text_content = []
            
            # Extract text from all pages
            for page_num, page in enumerate(pdf_reader.pages, 1):
                text = page.extract_text()
                if text:
                    # Add page separator for better context
                    text_content.append(f"--- Page {page_num} ---\n{text}")
            
            # Return complete text without any truncation
            full_text = "\n\n".join(text_content)
            return full_text
        
        except Exception as e:
            raise Exception(f"Failed to parse PDF: {str(e)}")
    
    async def _parse_docx(self, file_content: bytes) -> str:
        """
        Parse DOCX document and extract all text
        
        Args:
            file_content: DOCX file content as bytes
        
        Returns:
            Complete extracted text including tables
        """
        try:
            doc_file = io.BytesIO(file_content)
            doc = Document(doc_file)
            
            text_content = []
            
            # Extract all paragraph text
            for paragraph in doc.paragraphs:
                if paragraph.text.strip():
                    text_content.append(paragraph.text)
            
            # Extract all text from tables
            for table_num, table in enumerate(doc.tables, 1):
                table_text = [f"\n--- Table {table_num} ---"]
                for row in table.rows:
                    row_text = " | ".join(cell.text.strip() for cell in row.cells if cell.text.strip())
                    if row_text:
                        table_text.append(row_text)
                text_content.append("\n".join(table_text))
            
            # Return complete text without any truncation
            full_text = "\n\n".join(text_content)
            return full_text
        
        except Exception as e:
            raise Exception(f"Failed to parse DOCX document: {str(e)}")
    
    async def _parse_doc_legacy(self, file_content: bytes) -> str:
        """
        Parse legacy .doc file (Microsoft Word 97-2003 format)
        Uses olefile to extract text from OLE compound document
        
        Args:
            file_content: DOC file content as bytes
        
        Returns:
            Extracted text content
        """
        try:
            doc_file = io.BytesIO(file_content)
            
            # Check if it's an OLE file
            if not olefile.isOleFile(doc_file):
                raise Exception("File is not a valid Microsoft Word .doc file (OLE format)")
            
            # Reset stream position
            doc_file.seek(0)
            
            # Open OLE file
            ole = olefile.OleFileIO(doc_file)
            
            text_content = []
            
            try:
                # Method 1: Try to extract from WordDocument stream
                if ole.exists('WordDocument'):
                    word_stream = ole.openstream('WordDocument')
                    raw_data = word_stream.read()
                    
                    # Extract text using character filtering
                    # This is a simplified approach - full .doc parsing is very complex
                    extracted_chars = []
                    
                    i = 0
                    while i < len(raw_data):
                        byte = raw_data[i]
                        
                        # Printable ASCII characters
                        if 32 <= byte <= 126:
                            extracted_chars.append(chr(byte))
                        # Newline, tab, carriage return
                        elif byte in (9, 10, 13):
                            extracted_chars.append(chr(byte))
                        # Space
                        elif byte == 0:
                            # Multiple nulls might indicate word boundary
                            if i > 0 and raw_data[i-1] != 0:
                                extracted_chars.append(' ')
                        
                        i += 1
                    
                    extracted_text = ''.join(extracted_chars)
                    
                    # Clean up the extracted text
                    # Remove excessive whitespace
                    lines = extracted_text.split('\n')
                    cleaned_lines = []
                    
                    for line in lines:
                        # Remove control characters and excessive spaces
                        cleaned = re.sub(r'\s+', ' ', line.strip())
                        
                        # Only keep lines with meaningful content
                        # (at least 3 characters, contains letters)
                        if len(cleaned) >= 3 and any(c.isalpha() for c in cleaned):
                            cleaned_lines.append(cleaned)
                    
                    # Remove duplicates while preserving order
                    seen = set()
                    unique_lines = []
                    for line in cleaned_lines:
                        if line not in seen and len(line) > 5:  # Skip very short lines
                            seen.add(line)
                            unique_lines.append(line)
                    
                    text_content.extend(unique_lines)
                
                # Method 2: Try to extract from 1Table stream (contains formatting and text)
                if ole.exists('1Table'):
                    table_stream = ole.openstream('1Table')
                    table_data = table_stream.read()
                    
                    # Extract additional text from table stream
                    table_chars = []
                    for byte in table_data:
                        if 32 <= byte <= 126:
                            table_chars.append(chr(byte))
                        elif byte in (10, 13):
                            table_chars.append(' ')
                    
                    table_text = ''.join(table_chars)
                    table_lines = [
                        re.sub(r'\s+', ' ', line.strip())
                        for line in table_text.split()
                        if len(line.strip()) >= 3 and any(c.isalpha() for c in line)
                    ]
                    
                    # Add unique lines not already in text_content
                    for line in table_lines:
                        if line not in text_content and len(line) > 5:
                            text_content.append(line)
            
            finally:
                ole.close()
            
            if not text_content:
                raise Exception("No readable text content found in .doc file. The file may be corrupted, password-protected, or empty.")
            
            # Join all content
            full_text = "\n\n".join(text_content)
            
            # Add informational note
            note = "[NOTE: Text extracted from legacy Microsoft Word .doc format. Formatting and special characters may not be preserved. For best results, please convert to .docx or PDF format.]"
            
            return f"{note}\n\n{full_text}"
        
        except Exception as e:
            # Provide helpful error message
            error_msg = str(e)
            if "not a valid" in error_msg.lower():
                raise Exception(f"Failed to parse .doc file: {error_msg}")
            elif "no readable text" in error_msg.lower():
                raise Exception(error_msg)
            else:
                raise Exception(
                    f"Failed to parse legacy .doc file: {error_msg}. "
                    "This file may be password-protected, corrupted, or in an unsupported format. "
                    "Please try converting to .docx or PDF format."
                )
    
    async def _parse_word(self, file_content: bytes) -> str:
        """
        Deprecated: Use _parse_docx or _parse_doc_legacy instead
        Kept for backward compatibility
        """
        # Try to detect format and parse accordingly
        file_obj = io.BytesIO(file_content)
        
        # Check if it's OLE (old .doc)
        if olefile.isOleFile(file_obj):
            return await self._parse_doc_legacy(file_content)
        else:
            # Assume it's DOCX (ZIP-based)
            return await self._parse_docx(file_content)