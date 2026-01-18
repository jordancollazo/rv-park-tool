"""
Diligence Agent - Shared Utility Functions
Provides common functions for deal management, file operations, and JSON handling
"""

import os
import json
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional


def generate_deal_id() -> str:
    """
    Generate unique timestamp-based deal ID
    Format: YYYYMMDD_HHMMSS (e.g., 20260109_143022)

    Returns:
        Deal ID string
    """
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def ensure_deal_folders(deal_id: str) -> Dict[str, Path]:
    """
    Create deal folder structure if it doesn't exist

    Structure:
    .tmp/diligence/{deal_id}/
        ├── metadata.json
        ├── documents/
        │   ├── original/
        │   └── extracted/
        ├── analysis/
        ├── chat/
        └── exports/

    Args:
        deal_id: Deal identifier

    Returns:
        Dict of folder paths
    """
    base_path = Path(".tmp") / "diligence" / deal_id

    folders = {
        "base": base_path,
        "documents": base_path / "documents",
        "documents_original": base_path / "documents" / "original",
        "documents_extracted": base_path / "documents" / "extracted",
        "analysis": base_path / "analysis",
        "chat": base_path / "chat",
        "exports": base_path / "exports"
    }

    # Create all folders
    for folder_path in folders.values():
        folder_path.mkdir(parents=True, exist_ok=True)

    return folders


def save_json(file_path: str, data: Dict[str, Any]) -> bool:
    """
    Save data to JSON file with error handling

    Args:
        file_path: Path to save JSON file
        data: Dict to save

    Returns:
        True if successful, False otherwise
    """
    try:
        # Ensure parent directory exists
        Path(file_path).parent.mkdir(parents=True, exist_ok=True)

        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        return True
    except Exception as e:
        print(f"Error saving JSON to {file_path}: {e}")
        return False


def load_json(file_path: str, default: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Load data from JSON file with error handling

    Args:
        file_path: Path to JSON file
        default: Default value if file doesn't exist or can't be loaded

    Returns:
        Loaded dict or default value
    """
    if default is None:
        default = {}

    try:
        if not os.path.exists(file_path):
            return default

        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"Error loading JSON from {file_path}: {e}")
        return default


def validate_deal_id(deal_id: str) -> bool:
    """
    Check if deal_id exists and is valid

    Args:
        deal_id: Deal identifier to validate

    Returns:
        True if valid and exists, False otherwise
    """
    if not deal_id:
        return False

    deal_path = Path(".tmp") / "diligence" / deal_id
    return deal_path.exists() and deal_path.is_dir()


def get_all_deals() -> list[Dict[str, Any]]:
    """
    Get list of all deals with basic metadata

    Returns:
        List of deal dicts with metadata
    """
    deals = []
    diligence_path = Path(".tmp") / "diligence"

    if not diligence_path.exists():
        return deals

    for deal_dir in diligence_path.iterdir():
        if deal_dir.is_dir():
            deal_id = deal_dir.name
            metadata_path = deal_dir / "metadata.json"

            if metadata_path.exists():
                metadata = load_json(str(metadata_path))
                metadata["deal_id"] = deal_id
                deals.append(metadata)
            else:
                # Deal folder exists but no metadata
                deals.append({
                    "deal_id": deal_id,
                    "name": "Unknown Deal",
                    "status": "incomplete",
                    "created_at": None
                })

    # Sort by created_at (newest first)
    deals.sort(key=lambda x: x.get("created_at", ""), reverse=True)
    return deals


def format_currency(value: Optional[float]) -> str:
    """
    Format currency value for display

    Args:
        value: Dollar amount

    Returns:
        Formatted string (e.g., "$4,500,000")
    """
    if value is None:
        return "N/A"

    try:
        return f"${float(value):,.0f}"
    except (ValueError, TypeError):
        return "N/A"


def format_percentage(value: Optional[float], decimals: int = 1) -> str:
    """
    Format percentage value for display

    Args:
        value: Percentage value (e.g., 10.5 for 10.5%)
        decimals: Number of decimal places

    Returns:
        Formatted string (e.g., "10.5%")
    """
    if value is None:
        return "N/A"

    try:
        return f"{float(value):.{decimals}f}%"
    except (ValueError, TypeError):
        return "N/A"


def get_file_extension(file_path: str) -> str:
    """
    Get file extension in lowercase

    Args:
        file_path: Path to file

    Returns:
        Extension without dot (e.g., "pdf", "jpg")
    """
    return Path(file_path).suffix.lower().lstrip('.')


def is_pdf(file_path: str) -> bool:
    """Check if file is a PDF"""
    return get_file_extension(file_path) == 'pdf'


def is_image(file_path: str) -> bool:
    """Check if file is an image"""
    return get_file_extension(file_path) in ['jpg', 'jpeg', 'png', 'gif', 'bmp', 'tiff']


def is_excel(file_path: str) -> bool:
    """Check if file is an Excel file"""
    return get_file_extension(file_path) in ['xlsx', 'xls', 'csv']


def get_file_size_mb(file_path: str) -> float:
    """
    Get file size in megabytes

    Args:
        file_path: Path to file

    Returns:
        File size in MB
    """
    try:
        size_bytes = os.path.getsize(file_path)
        return size_bytes / (1024 * 1024)
    except Exception:
        return 0.0


if __name__ == "__main__":
    # Test utility functions
    print("Testing diligence_utils...")

    # Test deal ID generation
    deal_id = generate_deal_id()
    print(f"Generated deal_id: {deal_id}")

    # Test folder creation
    print(f"Creating folders for {deal_id}...")
    folders = ensure_deal_folders(deal_id)
    print(f"Created {len(folders)} folders")

    # Test JSON operations
    test_data = {"name": "Test Deal", "status": "created"}
    metadata_path = folders["base"] / "metadata.json"

    print(f"Saving test metadata...")
    success = save_json(str(metadata_path), test_data)
    print(f"Save successful: {success}")

    print(f"Loading metadata...")
    loaded_data = load_json(str(metadata_path))
    print(f"Loaded data: {loaded_data}")

    # Test validation
    print(f"Validating deal_id...")
    is_valid = validate_deal_id(deal_id)
    print(f"Deal valid: {is_valid}")

    # Test formatting
    print(f"Format currency: {format_currency(4500000)}")
    print(f"Format percentage: {format_percentage(10.5)}")

    print("\nAll utility tests passed!")
