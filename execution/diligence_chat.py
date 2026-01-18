"""
Diligence Agent - Interactive Q&A Chat
Provides conversational interface to ask questions about deals
"""

import os
import json
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List

# Import utilities
from diligence_utils import (
    validate_deal_id,
    save_json,
    load_json
)

# Claude API
try:
    from anthropic import Anthropic
    ANTHROPIC_AVAILABLE = True
except ImportError:
    ANTHROPIC_AVAILABLE = False
    print("Warning: anthropic not installed. Install with: pip install anthropic")


def start_chat_session(deal_id: str) -> Dict[str, Any]:
    """
    Initialize chat session for a deal

    Args:
        deal_id: Deal identifier

    Returns:
        Session info dict
    """
    if not validate_deal_id(deal_id):
        return {"success": False, "error": "Invalid deal_id"}

    chat_path = Path(".tmp") / "diligence" / deal_id / "chat"
    chat_path.mkdir(parents=True, exist_ok=True)

    history_file = chat_path / "chat_history.json"

    if not history_file.exists():
        # Initialize new chat history
        history = {
            "deal_id": deal_id,
            "started_at": datetime.now().isoformat(),
            "messages": []
        }
        save_json(str(history_file), history)

    # Update deal metadata
    from diligence_document_processor import get_deal_metadata, update_deal_metadata
    metadata = get_deal_metadata(deal_id)
    if not metadata.get("chat_started"):
        metadata["chat_started"] = True
        metadata["updated_at"] = datetime.now().isoformat()
        update_deal_metadata(deal_id, metadata)

    return {"success": True, "deal_id": deal_id}


def get_chat_history(deal_id: str) -> List[Dict[str, Any]]:
    """
    Load complete chat history for a deal

    Args:
        deal_id: Deal identifier

    Returns:
        List of message dicts
    """
    if not validate_deal_id(deal_id):
        return []

    history_file = Path(".tmp") / "diligence" / deal_id / "chat" / "chat_history.json"

    if not history_file.exists():
        return []

    history = load_json(str(history_file))
    return history.get("messages", [])


def build_chat_context(deal_id: str) -> str:
    """
    Build context for AI including deal analysis and documents

    Args:
        deal_id: Deal identifier

    Returns:
        Formatted context string
    """
    context_parts = []

    # Load analysis report if available
    analysis_file = Path(".tmp") / "diligence" / deal_id / "analysis" / "initial_report_raw.json"

    if analysis_file.exists():
        analysis = load_json(str(analysis_file))

        # Extract key info
        prop = analysis.get("property_overview", {})
        fin = analysis.get("financial_analysis", {})

        context_parts.append("DEAL SUMMARY:")
        context_parts.append(f"Property: {prop.get('name', 'Unknown')}")
        context_parts.append(f"Location: {prop.get('location', 'Unknown')}")
        context_parts.append(f"Units: {prop.get('units', 'N/A')}")
        context_parts.append(f"Purchase Price: ${prop.get('purchase_price', 0):,.0f}" if prop.get('purchase_price') else "Purchase Price: N/A")
        context_parts.append(f"NOI: ${fin.get('noi', 0):,.0f}" if fin.get('noi') else "NOI: N/A")
        context_parts.append(f"Cap Rate: {fin.get('cap_rate', 'N/A')}%")
        context_parts.append(f"Confidence Score: {analysis.get('confidence_score', 'N/A')}/100")

        context_parts.append("\nEXECUTIVE SUMMARY:")
        context_parts.append(analysis.get('executive_summary', 'No summary available'))

        # Include risk factors
        if analysis.get('risk_factors'):
            context_parts.append("\nKEY RISKS:")
            for risk in analysis['risk_factors'][:3]:  # Top 3 risks
                context_parts.append(f"- [{risk.get('severity', 'N/A').upper()}] {risk.get('description', 'N/A')}")

        # Include value-add opportunities
        if analysis.get('value_add_opportunities'):
            context_parts.append("\nVALUE-ADD OPPORTUNITIES:")
            for opp in analysis['value_add_opportunities'][:3]:  # Top 3 opportunities
                context_parts.append(f"- [{opp.get('impact', 'N/A').upper()}] {opp.get('description', 'N/A')}")

        # Include data gaps
        if analysis.get('data_gaps'):
            context_parts.append("\nDATA GAPS:")
            for gap in analysis['data_gaps'][:5]:
                context_parts.append(f"- {gap}")

    else:
        context_parts.append("No analysis report available yet. Please analyze the deal first.")

    # Document summary
    from diligence_document_processor import get_deal_documents
    documents = get_deal_documents(deal_id)

    if documents:
        context_parts.append(f"\nAVAILABLE DOCUMENTS ({len(documents)}):")
        for doc in documents:
            context_parts.append(f"- {doc['filename']} (Type: {doc['doc_type']}, Status: {doc['extraction_status']})")
    else:
        context_parts.append("\nNo documents uploaded yet.")

    return "\n".join(context_parts)


def send_chat_message(deal_id: str, user_message: str) -> Dict[str, Any]:
    """
    Send message to AI and get response

    Args:
        deal_id: Deal identifier
        user_message: User's question

    Returns:
        Dict with assistant response
    """
    if not validate_deal_id(deal_id):
        return {"success": False, "error": "Invalid deal_id"}

    if not ANTHROPIC_AVAILABLE:
        return {"success": False, "error": "Anthropic library not installed"}

    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        return {"success": False, "error": "ANTHROPIC_API_KEY not found"}

    if not user_message.strip():
        return {"success": False, "error": "Empty message"}

    try:
        # Load chat history
        history = get_chat_history(deal_id)

        # Build context
        deal_context = build_chat_context(deal_id)

        # System prompt
        system_prompt = f"""You are a commercial real estate analyst helping evaluate an MHP acquisition deal.

{deal_context}

Answer questions based on the deal analysis and documents. If information is not available in the deal data,
clearly state what data is missing. Provide actionable insights and specific recommendations.

Be concise but thorough. Use data and numbers to support your answers. If the user asks about something
that wasn't analyzed (like local market conditions, zoning, etc.), acknowledge the limitation and suggest
what additional research would be needed."""

        # Build conversation messages (last 10 for context)
        messages = []
        for msg in history[-10:]:
            messages.append({
                "role": msg["role"],
                "content": msg["content"]
            })

        # Add current user message
        messages.append({
            "role": "user",
            "content": user_message
        })

        # Call Claude API
        client = Anthropic(api_key=api_key)

        response = client.messages.create(
            model="claude-sonnet-4-5-20250929",
            max_tokens=2000,
            temperature=0.3,  # Slightly more creative for Q&A
            system=system_prompt,
            messages=messages
        )

        assistant_response = response.content[0].text.strip()

        # Save to chat history
        timestamp = datetime.now().isoformat()

        history.append({
            "role": "user",
            "content": user_message,
            "timestamp": timestamp
        })

        history.append({
            "role": "assistant",
            "content": assistant_response,
            "timestamp": timestamp
        })

        # Save updated history
        history_file = Path(".tmp") / "diligence" / deal_id / "chat" / "chat_history.json"
        chat_data = {
            "deal_id": deal_id,
            "started_at": load_json(str(history_file), {}).get("started_at", timestamp),
            "updated_at": timestamp,
            "messages": history
        }
        save_json(str(history_file), chat_data)

        # Update deal metadata
        from diligence_document_processor import get_deal_metadata, update_deal_metadata
        metadata = get_deal_metadata(deal_id)
        metadata["updated_at"] = timestamp
        update_deal_metadata(deal_id, metadata)

        return {
            "success": True,
            "response": assistant_response,
            "timestamp": timestamp
        }

    except Exception as e:
        print(f"Chat error: {e}")
        return {
            "success": False,
            "error": str(e)
        }


def condense_chat_context(deal_id: str) -> bool:
    """
    Condense long chat history to stay within token limits
    Summarizes old messages, keeps recent 10 verbatim

    Args:
        deal_id: Deal identifier

    Returns:
        True if successful
    """
    # TODO: Implement chat condensing for conversations >20 messages
    # For now, we just keep last 10 messages in send_chat_message()
    # Future: Use Claude to summarize old conversation and create condensed context

    history = get_chat_history(deal_id)

    if len(history) > 20:
        print(f"Warning: Chat history has {len(history)} messages. Consider implementing condensing.")

    return True


def clear_chat_history(deal_id: str) -> bool:
    """
    Clear chat history for a deal

    Args:
        deal_id: Deal identifier

    Returns:
        True if successful
    """
    if not validate_deal_id(deal_id):
        return False

    history_file = Path(".tmp") / "diligence" / deal_id / "chat" / "chat_history.json"

    if history_file.exists():
        # Reset history
        history = {
            "deal_id": deal_id,
            "started_at": datetime.now().isoformat(),
            "messages": []
        }
        return save_json(str(history_file), history)

    return True


if __name__ == "__main__":
    # Test chat module
    print("Testing diligence_chat...")

    # Would need a real deal_id to test
    # deal_id = "20260109_143022"
    # start_chat_session(deal_id)
    # response = send_chat_message(deal_id, "What is the cap rate?")
    # print(f"Response: {response}")

    print("Chat module loaded successfully!")
