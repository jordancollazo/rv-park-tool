"""
Stage 1: OCR & LLM Parsing for Financial Calculator
Extracts property listing details from screenshots using OCR + Claude API
"""

import os
import json
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional

# OCR libraries
try:
    import pytesseract
    from PIL import Image
    TESSERACT_AVAILABLE = True
except ImportError:
    TESSERACT_AVAILABLE = False
    print("Warning: pytesseract not installed. Install with: pip install pytesseract pillow")

# Claude API
try:
    from anthropic import Anthropic
    ANTHROPIC_AVAILABLE = True
except ImportError:
    ANTHROPIC_AVAILABLE = False
    print("Warning: anthropic not installed. Install with: pip install anthropic")


def extract_text_from_image(image_path: str) -> str:
    """
    Extract text from image using pytesseract OCR

    Args:
        image_path: Path to image file (PNG, JPG, etc.)

    Returns:
        Extracted text string
    """
    if not TESSERACT_AVAILABLE:
        raise RuntimeError("pytesseract not installed")

    try:
        image = Image.open(image_path)
        text = pytesseract.image_to_string(image)
        return text.strip()
    except Exception as e:
        print(f"OCR Error: {e}")
        return ""


def parse_listing_with_llm(ocr_text: str, api_key: Optional[str] = None) -> Dict[str, Any]:
    """
    Parse OCR text using Claude to extract structured listing data

    Args:
        ocr_text: Raw text from OCR
        api_key: Anthropic API key (or from env)

    Returns:
        Dict with extracted fields
    """
    if not ANTHROPIC_AVAILABLE:
        raise RuntimeError("anthropic library not installed")

    if not api_key:
        api_key = os.getenv("ANTHROPIC_API_KEY")

    if not api_key:
        raise ValueError("ANTHROPIC_API_KEY not found in environment")

    client = Anthropic(api_key=api_key)

    prompt = f"""You are a real estate data extraction specialist. Parse the following OCR text from a property listing screenshot and extract structured data.

OCR Text:
{ocr_text}

Extract the following fields (use "Unspecified" if not found, use null for numeric fields if not found):
- property_name: Name of the property
- description: Brief description (1-2 sentences)
- purchase_price: Dollar amount (numeric, no commas) or null
- unit_count: Number of units/lots/spaces (numeric) or null
- location: City, State or address
- noi: Net Operating Income if mentioned (numeric) or null
- cap_rate: Cap rate if mentioned (numeric, as percentage like 7.5) or null
- additional_metrics: Any other financial metrics mentioned (as dict)

Respond ONLY with valid JSON, no markdown formatting:
{{
  "property_name": "...",
  "description": "...",
  "purchase_price": 1234567 or null,
  "unit_count": 123 or null,
  "location": "...",
  "noi": 123456 or null,
  "cap_rate": 7.5 or null,
  "additional_metrics": {{}},
  "confidence": "high|medium|low",
  "raw_ocr_text": "..."
}}"""

    try:
        response = client.messages.create(
            model="claude-sonnet-4-5-20250929",
            max_tokens=2000,
            temperature=0,
            messages=[
                {"role": "user", "content": prompt}
            ]
        )

        response_text = response.content[0].text.strip()

        # Parse JSON response
        parsed = json.loads(response_text)
        parsed["raw_ocr_text"] = ocr_text

        return parsed

    except json.JSONDecodeError as e:
        print(f"Failed to parse LLM response as JSON: {e}")
        print(f"Response: {response_text}")
        # Return fallback structure
        return {
            "property_name": "Parse Error",
            "description": "Failed to parse listing",
            "purchase_price": None,
            "unit_count": None,
            "location": "Unknown",
            "noi": None,
            "cap_rate": None,
            "additional_metrics": {},
            "confidence": "low",
            "raw_ocr_text": ocr_text,
            "error": str(e)
        }

    except Exception as e:
        print(f"LLM API Error: {e}")
        raise


def process_listing_screenshot(image_path: str, output_dir: str = ".tmp/calculator_option_1/leads") -> Dict[str, Any]:
    """
    Full pipeline: OCR → LLM parsing → JSON output

    Args:
        image_path: Path to screenshot
        output_dir: Directory to save extracted JSON

    Returns:
        Extracted listing data dict
    """
    print(f"Processing: {image_path}")

    # Generate lead_id early so it's available even if OCR fails
    lead_id = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Step 1: OCR
    print("Running OCR...")
    ocr_text = extract_text_from_image(image_path)

    if not ocr_text:
        print("Warning: No text extracted from image - creating blank lead for manual entry")
        parsed_data = {
            "property_name": "Manual Entry Required",
            "description": "OCR failed - Please enter property details manually",
            "purchase_price": None,
            "unit_count": None,
            "location": "",
            "noi": None,
            "cap_rate": None,
            "additional_metrics": {},
            "confidence": "low",
            "raw_ocr_text": "",
            "error": "Tesseract OCR not available - manual entry required",
            "source_image": str(image_path),
            "processed_at": datetime.now().isoformat(),
            "lead_id": lead_id
        }

        # Save blank lead for manual entry
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, f"{lead_id}.json")
        with open(output_path, 'w') as f:
            json.dump(parsed_data, f, indent=2)

        print(f"Created blank lead for manual entry: {output_path}")
        return parsed_data

    print(f"Extracted {len(ocr_text)} characters")

    # Step 2: LLM Parsing
    print("Parsing with Claude API...")
    parsed_data = parse_listing_with_llm(ocr_text)

    # Step 3: Add metadata
    parsed_data["source_image"] = str(image_path)
    parsed_data["processed_at"] = datetime.now().isoformat()
    parsed_data["lead_id"] = lead_id

    # Step 4: Save to JSON
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, f"{parsed_data['lead_id']}.json")

    with open(output_path, 'w') as f:
        json.dump(parsed_data, f, indent=2)

    print(f"Saved to: {output_path}")
    print(f"Confidence: {parsed_data.get('confidence', 'unknown')}")

    return parsed_data


def main():
    """CLI entry point"""
    if len(sys.argv) < 2:
        print("Usage: python financial_calc_ocr.py <image_path>")
        print("Example: python financial_calc_ocr.py .tmp/calculator_option_1/uploads/listing.png")
        sys.exit(1)

    image_path = sys.argv[1]

    if not os.path.exists(image_path):
        print(f"Error: File not found: {image_path}")
        sys.exit(1)

    try:
        result = process_listing_screenshot(image_path)

        # Print summary
        print("\n" + "="*60)
        print("EXTRACTION SUMMARY")
        print("="*60)
        print(f"Property: {result.get('property_name', 'N/A')}")
        print(f"Location: {result.get('location', 'N/A')}")
        print(f"Price: ${result.get('purchase_price', 0):,}" if result.get('purchase_price') else "Price: Unspecified")
        print(f"Units: {result.get('unit_count', 'N/A')}")
        print(f"NOI: ${result.get('noi', 0):,}" if result.get('noi') else "NOI: N/A")
        print(f"Cap Rate: {result.get('cap_rate', 'N/A')}%" if result.get('cap_rate') else "Cap Rate: N/A")
        print(f"Confidence: {result.get('confidence', 'unknown')}")
        print("="*60)

    except Exception as e:
        print(f"Error processing image: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
