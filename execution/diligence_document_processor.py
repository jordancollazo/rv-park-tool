"""
Diligence Agent - Document Processing
Handles document uploads, text extraction (OCR), and metadata management
"""

import os
import shutil
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional, List

# Import utilities
from diligence_utils import (
    generate_deal_id,
    ensure_deal_folders,
    save_json,
    load_json,
    validate_deal_id,
    is_pdf,
    is_image,
    is_excel,
    get_file_size_mb
)

# PDF processing
try:
    import PyPDF2
    PYPDF2_AVAILABLE = True
except ImportError:
    PYPDF2_AVAILABLE = False
    print("Warning: PyPDF2 not installed. Install with: pip install PyPDF2")

# OCR libraries
try:
    import pytesseract
    from PIL import Image
    TESSERACT_AVAILABLE = True
except ImportError:
    TESSERACT_AVAILABLE = False
    print("Warning: pytesseract not installed. Install with: pip install pytesseract pillow")

# Excel processing
try:
    import openpyxl
    import pandas as pd
    EXCEL_AVAILABLE = True
except ImportError:
    EXCEL_AVAILABLE = False
    print("Warning: openpyxl/pandas not installed. Install with: pip install openpyxl pandas")


def create_deal(deal_name: str, lead_id: Optional[int] = None) -> str:
    """
    Create new deal folder with metadata

    Args:
        deal_name: Name of the deal (e.g., "Sunset MHP")
        lead_id: Optional CRM lead ID to link

    Returns:
        Deal ID string
    """
    # Generate unique deal ID
    deal_id = generate_deal_id()

    # Create folder structure
    folders = ensure_deal_folders(deal_id)

    # Create metadata
    metadata = {
        "deal_id": deal_id,
        "name": deal_name,
        "lead_id": lead_id,
        "status": "created",
        "created_at": datetime.now().isoformat(),
        "updated_at": datetime.now().isoformat(),
        "documents": [],
        "analysis_completed": False,
        "chat_started": False
    }

    # Save metadata
    metadata_path = folders["base"] / "metadata.json"
    save_json(str(metadata_path), metadata)

    print(f"Created deal: {deal_name} (ID: {deal_id})")
    return deal_id


def extract_text_from_pdf(pdf_path: str) -> Dict[str, Any]:
    """
    Extract text from PDF using PyPDF2
    Falls back to OCR if PDF is scanned (no text layer)

    Args:
        pdf_path: Path to PDF file

    Returns:
        Dict with extracted text and metadata
    """
    result = {
        "text": "",
        "page_count": 0,
        "extraction_method": "none",
        "confidence": "low",
        "error": None
    }

    if not PYPDF2_AVAILABLE:
        result["error"] = "PyPDF2 not installed"
        return result

    try:
        # Try PyPDF2 first (fast, works for PDFs with text layer)
        with open(pdf_path, 'rb') as f:
            pdf_reader = PyPDF2.PdfReader(f)
            result["page_count"] = len(pdf_reader.pages)

            text_parts = []
            for page in pdf_reader.pages:
                page_text = page.extract_text()
                if page_text:
                    text_parts.append(page_text)

            result["text"] = "\n\n".join(text_parts)

        # Check if we got meaningful text
        if len(result["text"].strip()) > 100:
            result["extraction_method"] = "pypdf2"
            result["confidence"] = "high"
            print(f"  Extracted {len(result['text'])} characters from {result['page_count']} pages using PyPDF2")
            return result

        # No text extracted - likely scanned PDF, try OCR
        print(f"  PDF appears to be scanned (no text layer), falling back to OCR...")
        return extract_text_from_scanned_pdf(pdf_path, result["page_count"])

    except Exception as e:
        result["error"] = str(e)
        print(f"  PDF extraction error: {e}")
        return result


def extract_text_from_scanned_pdf(pdf_path: str, page_count: int) -> Dict[str, Any]:
    """
    Extract text from scanned PDF using OCR
    (Placeholder for future implementation - requires PDF to image conversion)

    Args:
        pdf_path: Path to PDF file
        page_count: Number of pages in PDF

    Returns:
        Dict with extracted text and metadata
    """
    result = {
        "text": "",
        "page_count": page_count,
        "extraction_method": "ocr_placeholder",
        "confidence": "low",
        "error": "OCR for scanned PDFs not yet implemented - please provide image files or text-based PDFs"
    }

    # TODO: Implement PDF to image conversion + OCR
    # Requires: pdf2image library (poppler dependency)
    # from pdf2image import convert_from_path
    # images = convert_from_path(pdf_path)
    # for img in images:
    #     text += pytesseract.image_to_string(img)

    return result


def extract_text_from_image(image_path: str) -> Dict[str, Any]:
    """
    Extract text from image using pytesseract OCR
    Follows pattern from financial_calc_ocr.py

    Args:
        image_path: Path to image file (PNG, JPG, etc.)

    Returns:
        Dict with extracted text and metadata
    """
    result = {
        "text": "",
        "extraction_method": "none",
        "confidence": "low",
        "error": None
    }

    if not TESSERACT_AVAILABLE:
        result["error"] = "pytesseract not installed"
        return result

    try:
        image = Image.open(image_path)
        text = pytesseract.image_to_string(image)
        result["text"] = text.strip()

        # Estimate confidence based on text length and quality
        if len(result["text"]) > 500:
            result["confidence"] = "high"
        elif len(result["text"]) > 100:
            result["confidence"] = "medium"
        else:
            result["confidence"] = "low"

        result["extraction_method"] = "pytesseract"
        print(f"  Extracted {len(result['text'])} characters via OCR (confidence: {result['confidence']})")

    except Exception as e:
        result["error"] = str(e)
        print(f"  OCR error: {e}")

    return result


def extract_text_from_excel(excel_path: str) -> Dict[str, Any]:
    """
    Extract data from Excel/CSV files

    Args:
        excel_path: Path to Excel/CSV file

    Returns:
        Dict with extracted text and metadata
    """
    result = {
        "text": "",
        "extraction_method": "none",
        "confidence": "high",
        "error": None
    }

    if not EXCEL_AVAILABLE:
        result["error"] = "openpyxl/pandas not installed"
        return result

    try:
        # Use pandas to read Excel/CSV
        if excel_path.endswith('.csv'):
            df = pd.read_csv(excel_path)
        else:
            df = pd.read_excel(excel_path)

        # Convert to text representation
        result["text"] = df.to_string()
        result["extraction_method"] = "pandas"

        print(f"  Extracted {len(df)} rows x {len(df.columns)} columns from Excel file")

    except Exception as e:
        result["error"] = str(e)
        print(f"  Excel extraction error: {e}")

    return result


def upload_document(deal_id: str, file_path: str, doc_type: str) -> Dict[str, Any]:
    """
    Upload document to deal and extract text

    Args:
        deal_id: Deal identifier
        file_path: Path to document file
        doc_type: Document type ("offering_memo", "financials", "photos", "legal", "other")

    Returns:
        Dict with upload result and extracted metadata
    """
    if not validate_deal_id(deal_id):
        return {"success": False, "error": f"Invalid deal_id: {deal_id}"}

    if not os.path.exists(file_path):
        return {"success": False, "error": f"File not found: {file_path}"}

    try:
        # Get file info
        filename = Path(file_path).name
        file_size_mb = get_file_size_mb(file_path)

        print(f"\nProcessing document: {filename} ({file_size_mb:.2f} MB)")

        # Copy to documents/original/
        folders = ensure_deal_folders(deal_id)
        dest_path = folders["documents_original"] / filename
        shutil.copy2(file_path, dest_path)

        # Extract text based on file type
        if is_pdf(file_path):
            extraction = extract_text_from_pdf(str(dest_path))
        elif is_image(file_path):
            extraction = extract_text_from_image(str(dest_path))
        elif is_excel(file_path):
            extraction = extract_text_from_excel(str(dest_path))
        else:
            extraction = {
                "text": "",
                "extraction_method": "unsupported",
                "confidence": "n/a",
                "error": f"Unsupported file type: {Path(file_path).suffix}"
            }

        # Save extracted text
        extracted_filename = Path(filename).stem + ".txt"
        extracted_path = folders["documents_extracted"] / extracted_filename
        with open(extracted_path, 'w', encoding='utf-8') as f:
            f.write(extraction["text"])

        # Save extraction metadata
        metadata_filename = Path(filename).stem + "_metadata.json"
        metadata_path = folders["documents_extracted"] / metadata_filename
        extraction_metadata = {
            "filename": filename,
            "doc_type": doc_type,
            "file_size_mb": file_size_mb,
            "uploaded_at": datetime.now().isoformat(),
            "extraction_method": extraction["extraction_method"],
            "confidence": extraction["confidence"],
            "text_length": len(extraction["text"]),
            "error": extraction.get("error")
        }
        save_json(str(metadata_path), extraction_metadata)

        # Update deal metadata
        deal_metadata = get_deal_metadata(deal_id)
        deal_metadata["documents"].append({
            "filename": filename,
            "doc_type": doc_type,
            "uploaded_at": datetime.now().isoformat(),
            "extraction_status": "success" if extraction["text"] else "failed",
            "confidence": extraction["confidence"]
        })
        deal_metadata["updated_at"] = datetime.now().isoformat()
        deal_metadata["status"] = "documents_uploaded"
        update_deal_metadata(deal_id, deal_metadata)

        return {
            "success": True,
            "filename": filename,
            "doc_type": doc_type,
            "extraction": extraction_metadata
        }

    except Exception as e:
        print(f"Error uploading document: {e}")
        return {"success": False, "error": str(e)}


def get_deal_documents(deal_id: str) -> List[Dict[str, Any]]:
    """
    Get list of all documents for a deal

    Args:
        deal_id: Deal identifier

    Returns:
        List of document metadata dicts
    """
    if not validate_deal_id(deal_id):
        return []

    metadata = get_deal_metadata(deal_id)
    return metadata.get("documents", [])


def get_deal_metadata(deal_id: str) -> Dict[str, Any]:
    """
    Load deal metadata.json

    Args:
        deal_id: Deal identifier

    Returns:
        Metadata dict
    """
    if not validate_deal_id(deal_id):
        return {}

    metadata_path = Path(".tmp") / "diligence" / deal_id / "metadata.json"
    return load_json(str(metadata_path))


def update_deal_metadata(deal_id: str, updates: Dict[str, Any]) -> bool:
    """
    Update deal metadata

    Args:
        deal_id: Deal identifier
        updates: Dict with fields to update

    Returns:
        True if successful
    """
    if not validate_deal_id(deal_id):
        return False

    metadata_path = Path(".tmp") / "diligence" / deal_id / "metadata.json"
    return save_json(str(metadata_path), updates)


if __name__ == "__main__":
    # Test document processor
    print("Testing diligence_document_processor...")

    # Test deal creation
    deal_id = create_deal("Test Deal - Sunset MHP", lead_id=123)

    print(f"\nDeal created with ID: {deal_id}")
    print(f"Metadata: {get_deal_metadata(deal_id)}")

    # Test document upload (would need actual files)
    # upload_document(deal_id, "test.pdf", "offering_memo")

    print("\nDocument processor tests complete!")
