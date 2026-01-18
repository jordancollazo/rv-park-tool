"""
Diligence Agent - AI Analysis & Report Generation
Generates comprehensive investment analysis reports using Claude API
"""

import os
import json
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional, List

# Import utilities
from diligence_utils import (
    validate_deal_id,
    save_json,
    load_json,
    format_currency,
    format_percentage
)

# Claude API
try:
    from anthropic import Anthropic
    ANTHROPIC_AVAILABLE = True
except ImportError:
    ANTHROPIC_AVAILABLE = False
    print("Warning: anthropic not installed. Install with: pip install anthropic")


def build_document_context(deal_id: str, max_chars: int = 150000) -> str:
    """
    Build context string from all extracted documents
    Prioritizes offering memos and financials

    Args:
        deal_id: Deal identifier
        max_chars: Maximum characters to include (stay within token limits)

    Returns:
        Formatted context string
    """
    if not validate_deal_id(deal_id):
        return ""

    extracted_path = Path(".tmp") / "diligence" / deal_id / "documents" / "extracted"

    if not extracted_path.exists():
        return "No documents uploaded yet."

    context_parts = []
    total_chars = 0

    # Group files by type
    doc_priorities = {
        "offering_memo": [],
        "financials": [],
        "legal": [],
        "photos": [],
        "other": []
    }

    # Load document metadata to determine types
    for metadata_file in extracted_path.glob("*_metadata.json"):
        metadata = load_json(str(metadata_file))
        doc_type = metadata.get("doc_type", "other")
        filename = metadata.get("filename", "")
        text_filename = Path(metadata_file.stem).stem + ".txt"

        if doc_type in doc_priorities:
            doc_priorities[doc_type].append({
                "filename": filename,
                "text_file": text_filename,
                "confidence": metadata.get("confidence", "medium")
            })

    # Build context in priority order
    for doc_type in ["offering_memo", "financials", "legal", "photos", "other"]:
        for doc in doc_priorities[doc_type]:
            text_path = extracted_path / doc["text_file"]

            if text_path.exists():
                with open(text_path, 'r', encoding='utf-8') as f:
                    text = f.read()

                # Add document header
                header = f"\n\n--- DOCUMENT: {doc['filename']} (Type: {doc_type}, Confidence: {doc['confidence']}) ---\n"
                content = header + text

                # Check if we have room
                if total_chars + len(content) > max_chars:
                    # Truncate if needed
                    remaining = max_chars - total_chars
                    if remaining > 1000:
                        content = content[:remaining] + "\n\n[Document truncated due to length...]"
                    else:
                        break

                context_parts.append(content)
                total_chars += len(content)

            if total_chars >= max_chars:
                break

        if total_chars >= max_chars:
            break

    if not context_parts:
        return "No text extracted from uploaded documents."

    return "\n".join(context_parts)


def get_crm_lead_context(deal_id: str) -> str:
    """
    Get CRM lead data if deal is linked to a lead
    Imports CRM lookup functions dynamically to avoid circular dependencies

    Args:
        deal_id: Deal identifier

    Returns:
        Formatted CRM context string
    """
    try:
        from diligence_crm_lookup import get_lead_data_for_deal
        lead_data = get_lead_data_for_deal(deal_id)

        if not lead_data:
            return "No CRM lead data linked."

        # Format key fields
        context = "CRM Lead Data:\n"
        context += f"- Property Name: {lead_data.get('name', 'N/A')}\n"
        context += f"- Location: {lead_data.get('city', 'N/A')}, {lead_data.get('state', 'N/A')}\n"
        context += f"- Address: {lead_data.get('address', 'N/A')}\n"

        if lead_data.get('asking_price'):
            context += f"- Asking Price: {format_currency(lead_data['asking_price'])}\n"
        if lead_data.get('lot_count'):
            context += f"- Lot Count: {lead_data['lot_count']}\n"
        if lead_data.get('cap_rate'):
            context += f"- Cap Rate: {format_percentage(lead_data['cap_rate'])}\n"
        if lead_data.get('noi'):
            context += f"- NOI: {format_currency(lead_data['noi'])}\n"

        # Add scores if available
        if lead_data.get('owner_fatigue_score_0_100'):
            context += f"- Owner Fatigue Score: {lead_data['owner_fatigue_score_0_100']}/100\n"
        if lead_data.get('tax_shock_score_0_100'):
            context += f"- Tax Shock Score: {lead_data['tax_shock_score_0_100']}/100\n"
        if lead_data.get('insurance_pressure_score_0_100'):
            context += f"- Insurance Pressure Score: {lead_data['insurance_pressure_score_0_100']}/100\n"

        return context

    except ImportError:
        return "CRM integration not available."
    except Exception as e:
        print(f"Error fetching CRM data: {e}")
        return "Error fetching CRM lead data."


def analyze_deal(deal_id: str) -> Dict[str, Any]:
    """
    Generate comprehensive AI analysis report
    Follows Claude API pattern from financial_calc_ocr.py

    Args:
        deal_id: Deal identifier

    Returns:
        Analysis data dict
    """
    if not validate_deal_id(deal_id):
        return {"error": "Invalid deal_id"}

    if not ANTHROPIC_AVAILABLE:
        return {"error": "Anthropic library not installed"}

    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        return {"error": "ANTHROPIC_API_KEY not found in environment"}

    print(f"\nAnalyzing deal {deal_id}...")

    # Load documents and CRM context
    print("  Loading documents...")
    documents_context = build_document_context(deal_id)

    print("  Loading CRM data...")
    crm_context = get_crm_lead_context(deal_id)

    # Build AI prompt
    system_prompt = """You are a commercial real estate analyst specializing in mobile home park (MHP) acquisitions.
You analyze deal documents to provide comprehensive investment insights including financial performance,
risk assessment, deal structure, and value-add opportunities.

You must provide actionable, data-driven analysis. Be specific about numbers, calculations, and assumptions.
Identify red flags clearly and suggest mitigation strategies. Highlight value-add opportunities with estimated upside."""

    user_prompt = f"""Analyze the following deal documents and provide comprehensive investment analysis for an MHP acquisition.

DOCUMENTS:
{documents_context}

{crm_context}

Provide your analysis in the following JSON structure. Use null for numeric fields if data is not available.

CRITICAL: Respond with ONLY the raw JSON object. Do NOT wrap it in markdown code blocks or any other formatting. Your response must start with {{ and end with }}.

{{
  "executive_summary": "2-3 sentence high-level summary of the investment opportunity",
  "property_overview": {{
    "name": "Property name",
    "location": "City, State",
    "units": 123,
    "purchase_price": 4500000,
    "asking_price": 4500000,
    "year_built": 1985,
    "lot_size_acres": 25.5,
    "occupancy_rate": 95.0
  }},
  "financial_analysis": {{
    "noi": 450000,
    "gross_income": 650000,
    "operating_expenses": 200000,
    "cap_rate": 10.0,
    "price_per_unit": 36585,
    "cash_on_cash_return": 12.5,
    "dscr": 1.35,
    "expense_ratio": 30.8,
    "key_metrics": [
      "Cap rate of 10.0% indicates strong cash flow relative to price",
      "Operating expense ratio of 30.8% is healthy for MHP sector (target: 30-40%)",
      "95% occupancy suggests strong demand and good management"
    ]
  }},
  "deal_structure": {{
    "financing_type": "Bank financing",
    "down_payment_pct": 25,
    "interest_rate": 6.5,
    "loan_term_years": 20,
    "seller_financing": false,
    "contingencies": ["Inspection", "Financing", "Environmental"],
    "closing_timeline_days": 60
  }},
  "risk_factors": [
    {{
      "category": "Financial",
      "severity": "medium",
      "description": "Current occupancy is high but may be difficult to maintain",
      "mitigation": "Implement proactive tenant retention program and amenity upgrades"
    }},
    {{
      "category": "Operational",
      "severity": "low",
      "description": "Aging infrastructure may require capital improvements",
      "mitigation": "Budget $50k for deferred maintenance in year 1"
    }}
  ],
  "value_add_opportunities": [
    {{
      "category": "Revenue",
      "impact": "high",
      "description": "Lot rents are $50/month below market",
      "estimated_upside": "$61,500/year (123 units × $50/month × 12 months)",
      "implementation_timeline": "6-12 months (gradual increases)"
    }},
    {{
      "category": "Expense",
      "impact": "medium",
      "description": "Utilities are park-paid, can convert to tenant-paid",
      "estimated_upside": "$30,000/year in utility cost savings",
      "implementation_timeline": "12-18 months"
    }}
  ],
  "red_flags": [
    "Missing 3 years of historical financial statements - requires verification",
    "Environmental Phase I report not included - must obtain before closing",
    "Seller's cap rate calculation differs from broker's - verify actual NOI"
  ],
  "confidence_score": 75,
  "data_gaps": [
    "Historical financial performance (3+ years)",
    "Tenant payment history and delinquency rates",
    "Capital expenditure records",
    "Environmental assessment reports",
    "Utility expense breakdown"
  ],
  "next_steps": [
    "Request 3 years of financial statements and rent rolls",
    "Schedule property inspection to assess deferred maintenance",
    "Obtain Phase I environmental report",
    "Interview current park manager about operations",
    "Analyze local market comps to verify rental rates"
  ]
}}"""

    try:
        print("  Calling Claude API...")
        client = Anthropic(api_key=api_key)

        response = client.messages.create(
            model="claude-sonnet-4-5-20250929",
            max_tokens=4000,
            temperature=0,
            system=system_prompt,
            messages=[{"role": "user", "content": user_prompt}]
        )

        response_text = response.content[0].text.strip()

        # Parse JSON response
        print("  Parsing AI response...")

        # Remove markdown code blocks if present
        if response_text.startswith("```json"):
            response_text = response_text.replace("```json", "").replace("```", "").strip()
        elif response_text.startswith("```"):
            response_text = response_text.replace("```", "").strip()

        analysis_data = json.loads(response_text)

        # Add metadata
        analysis_data["deal_id"] = deal_id
        analysis_data["analyzed_at"] = datetime.now().isoformat()
        analysis_data["model_used"] = "claude-sonnet-4-5-20250929"

        # Save raw analysis
        analysis_path = Path(".tmp") / "diligence" / deal_id / "analysis"
        save_json(str(analysis_path / "initial_report_raw.json"), analysis_data)

        # Generate markdown report
        print("  Generating markdown report...")
        markdown_report = generate_report_markdown(analysis_data)
        with open(analysis_path / "initial_report.md", 'w', encoding='utf-8') as f:
            f.write(markdown_report)

        # Update deal metadata
        from diligence_document_processor import get_deal_metadata, update_deal_metadata
        metadata = get_deal_metadata(deal_id)
        metadata["analysis_completed"] = True
        metadata["status"] = "analyzed"
        metadata["updated_at"] = datetime.now().isoformat()
        update_deal_metadata(deal_id, metadata)

        print(f"  Analysis complete! Confidence score: {analysis_data.get('confidence_score', 'N/A')}/100")

        return analysis_data

    except json.JSONDecodeError as e:
        print(f"Failed to parse AI response as JSON: {e}")
        print(f"Response: {response_text[:500]}...")

        # Return fallback structure (pattern from financial_calc_ocr.py)
        return {
            "executive_summary": "Parse Error - Failed to analyze deal",
            "confidence_score": 0,
            "error": f"JSON parse error: {str(e)}",
            "raw_response": response_text[:1000]
        }

    except Exception as e:
        print(f"Analysis error: {e}")
        return {
            "executive_summary": "Analysis Error",
            "confidence_score": 0,
            "error": str(e)
        }


def generate_report_markdown(analysis_data: Dict[str, Any]) -> str:
    """
    Generate formatted markdown report from analysis data

    Args:
        analysis_data: Analysis dict from AI

    Returns:
        Markdown report string
    """
    prop = analysis_data.get("property_overview", {})
    fin = analysis_data.get("financial_analysis", {})
    deal_struct = analysis_data.get("deal_structure", {})

    md = f"""# Deal Analysis: {prop.get('name', 'Unknown Property')}

**Location**: {prop.get('location', 'N/A')}
**Analyzed**: {datetime.now().strftime('%B %d, %Y at %I:%M %p')}
**Confidence Score**: {analysis_data.get('confidence_score', 'N/A')}/100

---

## Executive Summary

{analysis_data.get('executive_summary', 'No summary available.')}

---

## Property Overview

- **Units/Lots**: {prop.get('units', 'N/A')}
- **Purchase Price**: {format_currency(prop.get('purchase_price'))}
- **Year Built**: {prop.get('year_built', 'N/A')}
- **Lot Size**: {prop.get('lot_size_acres', 'N/A')} acres
- **Current Occupancy**: {format_percentage(prop.get('occupancy_rate'))}

---

## Financial Analysis

### Key Metrics

- **NOI**: {format_currency(fin.get('noi'))}
- **Gross Income**: {format_currency(fin.get('gross_income'))}
- **Operating Expenses**: {format_currency(fin.get('operating_expenses'))}
- **Cap Rate**: {format_percentage(fin.get('cap_rate'))}
- **Price Per Unit**: {format_currency(fin.get('price_per_unit'))}
- **Cash-on-Cash Return**: {format_percentage(fin.get('cash_on_cash_return'))}
- **DSCR**: {fin.get('dscr', 'N/A')}
- **Expense Ratio**: {format_percentage(fin.get('expense_ratio'))}

### Analysis Insights

"""

    for metric in fin.get('key_metrics', []):
        md += f"- {metric}\n"

    md += "\n---\n\n## Deal Structure\n\n"
    md += f"- **Financing Type**: {deal_struct.get('financing_type', 'N/A')}\n"
    md += f"- **Down Payment**: {format_percentage(deal_struct.get('down_payment_pct'))}\n"
    md += f"- **Interest Rate**: {format_percentage(deal_struct.get('interest_rate'))}\n"
    md += f"- **Loan Term**: {deal_struct.get('loan_term_years', 'N/A')} years\n"
    md += f"- **Seller Financing**: {'Yes' if deal_struct.get('seller_financing') else 'No'}\n"
    md += f"- **Closing Timeline**: {deal_struct.get('closing_timeline_days', 'N/A')} days\n"

    if deal_struct.get('contingencies'):
        md += f"\n**Contingencies**: {', '.join(deal_struct['contingencies'])}\n"

    md += "\n---\n\n## Risk Factors\n\n"

    for risk in analysis_data.get('risk_factors', []):
        severity_badge = {"low": "🟢", "medium": "🟡", "high": "🔴"}.get(risk.get('severity', 'low'), "⚪")
        md += f"### {severity_badge} {risk.get('category', 'Unknown')} - {risk.get('severity', 'low').upper()}\n\n"
        md += f"{risk.get('description', 'No description')}\n\n"
        md += f"**Mitigation**: {risk.get('mitigation', 'No mitigation provided')}\n\n"

    md += "---\n\n## Value-Add Opportunities\n\n"

    for opp in analysis_data.get('value_add_opportunities', []):
        impact_badge = {"low": "↗️", "medium": "⬆️", "high": "🚀"}.get(opp.get('impact', 'low'), "📈")
        md += f"### {impact_badge} {opp.get('category', 'Unknown')} - {opp.get('impact', 'low').upper()} IMPACT\n\n"
        md += f"{opp.get('description', 'No description')}\n\n"
        md += f"**Estimated Upside**: {opp.get('estimated_upside', 'TBD')}\n\n"
        md += f"**Timeline**: {opp.get('implementation_timeline', 'TBD')}\n\n"

    md += "---\n\n## Red Flags\n\n"

    for flag in analysis_data.get('red_flags', []):
        md += f"- ⚠️ {flag}\n"

    md += "\n---\n\n## Data Gaps\n\n"

    for gap in analysis_data.get('data_gaps', []):
        md += f"- ❓ {gap}\n"

    md += "\n---\n\n## Recommended Next Steps\n\n"

    for i, step in enumerate(analysis_data.get('next_steps', []), 1):
        md += f"{i}. {step}\n"

    md += f"\n---\n\n*Report generated by Deal Diligence Agent on {datetime.now().strftime('%B %d, %Y at %I:%M %p')}*\n"

    return md


if __name__ == "__main__":
    # Test analyzer
    print("Testing diligence_analyzer...")

    # Would need a real deal_id with documents to test
    # deal_id = "20260109_143022"
    # analysis = analyze_deal(deal_id)
    # print(f"Analysis: {analysis}")

    print("Analyzer module loaded successfully!")
