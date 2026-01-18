"""
Diligence Agent - Flask Web Server
Provides API endpoints and serves frontend UI for deal diligence
"""

import os
from pathlib import Path
from flask import Flask, jsonify, request, send_from_directory, render_template_string
from werkzeug.utils import secure_filename

# Import diligence modules
from diligence_utils import get_all_deals, validate_deal_id
from diligence_document_processor import (
    create_deal,
    upload_document,
    get_deal_documents,
    get_deal_metadata
)
from diligence_analyzer import analyze_deal
from diligence_chat import (
    start_chat_session,
    send_chat_message,
    get_chat_history,
    clear_chat_history
)
from diligence_crm_lookup import (
    link_deal_to_lead,
    get_lead_data_for_deal,
    update_lead_with_diligence,
    get_deals_for_lead
)

# Initialize Flask app
app = Flask(__name__, static_folder='../static')
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024  # 50MB max file size

# Upload folder
UPLOAD_FOLDER = Path(".tmp") / "diligence_uploads"
UPLOAD_FOLDER.mkdir(parents=True, exist_ok=True)


# =============================================================================
# API ENDPOINTS
# =============================================================================

@app.route("/api/diligence/deals", methods=["GET"])
def api_list_deals():
    """List all deals with metadata"""
    try:
        deals = get_all_deals()
        return jsonify({"success": True, "deals": deals})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/diligence/deals", methods=["POST"])
def api_create_deal():
    """
    Create new deal
    Body: {"name": "Sunset MHP", "lead_id": 123}
    """
    try:
        data = request.json
        deal_name = data.get("name")
        lead_id = data.get("lead_id")

        if not deal_name:
            return jsonify({"success": False, "error": "Deal name required"}), 400

        deal_id = create_deal(deal_name, lead_id)

        # If lead_id provided, link to CRM
        if lead_id:
            link_deal_to_lead(deal_id, lead_id)

        return jsonify({
            "success": True,
            "deal_id": deal_id,
            "message": f"Created deal: {deal_name}"
        })

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/diligence/deals/<deal_id>", methods=["GET"])
def api_get_deal(deal_id):
    """Get deal details, documents, analysis"""
    try:
        if not validate_deal_id(deal_id):
            return jsonify({"success": False, "error": "Deal not found"}), 404

        metadata = get_deal_metadata(deal_id)
        documents = get_deal_documents(deal_id)

        # Load analysis if available
        analysis_file = Path(".tmp") / "diligence" / deal_id / "analysis" / "initial_report_raw.json"
        analysis = None
        if analysis_file.exists():
            import json
            with open(analysis_file, 'r', encoding='utf-8') as f:
                analysis = json.load(f)

        # Load chat history
        chat_history = get_chat_history(deal_id)

        # Get CRM lead data if linked
        lead_data = get_lead_data_for_deal(deal_id)

        return jsonify({
            "success": True,
            "deal": {
                "metadata": metadata,
                "documents": documents,
                "analysis": analysis,
                "chat_messages": len(chat_history),
                "lead_data": lead_data
            }
        })

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/diligence/deals/<deal_id>/documents", methods=["POST"])
def api_upload_document(deal_id):
    """
    Upload document to deal
    Form data: file, doc_type
    """
    try:
        if not validate_deal_id(deal_id):
            return jsonify({"success": False, "error": "Deal not found"}), 404

        if 'file' not in request.files:
            return jsonify({"success": False, "error": "No file provided"}), 400

        file = request.files['file']
        doc_type = request.form.get('doc_type', 'other')

        if file.filename == '':
            return jsonify({"success": False, "error": "No file selected"}), 400

        # Secure filename and save to temp location
        filename = secure_filename(file.filename)
        temp_path = UPLOAD_FOLDER / filename
        file.save(temp_path)

        # Process document
        result = upload_document(deal_id, str(temp_path), doc_type)

        # Clean up temp file
        temp_path.unlink()

        if result.get("success"):
            return jsonify(result)
        else:
            return jsonify(result), 500

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/diligence/deals/<deal_id>/analyze", methods=["POST"])
def api_analyze_deal(deal_id):
    """Trigger AI analysis"""
    try:
        if not validate_deal_id(deal_id):
            return jsonify({"success": False, "error": "Deal not found"}), 404

        # Check if documents are uploaded
        documents = get_deal_documents(deal_id)
        if not documents:
            return jsonify({
                "success": False,
                "error": "No documents uploaded. Please upload documents before analyzing."
            }), 400

        # Run analysis
        analysis = analyze_deal(deal_id)

        if analysis.get("error"):
            return jsonify({
                "success": False,
                "error": analysis["error"]
            }), 500

        # Update CRM if linked
        metadata = get_deal_metadata(deal_id)
        if metadata.get("lead_id"):
            update_lead_with_diligence(metadata["lead_id"], deal_id, analysis)

        return jsonify({
            "success": True,
            "analysis": analysis,
            "message": "Analysis complete!"
        })

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/diligence/deals/<deal_id>/report", methods=["GET"])
def api_get_report(deal_id):
    """Get markdown report"""
    try:
        if not validate_deal_id(deal_id):
            return jsonify({"success": False, "error": "Deal not found"}), 404

        report_file = Path(".tmp") / "diligence" / deal_id / "analysis" / "initial_report.md"

        if not report_file.exists():
            return jsonify({
                "success": False,
                "error": "No analysis report available. Please analyze the deal first."
            }), 404

        with open(report_file, 'r', encoding='utf-8') as f:
            report_markdown = f.read()

        return jsonify({
            "success": True,
            "report": report_markdown
        })

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/diligence/deals/<deal_id>/chat", methods=["POST"])
def api_chat_message(deal_id):
    """
    Send chat message, get AI response
    Body: {"message": "What is the cap rate?"}
    """
    try:
        if not validate_deal_id(deal_id):
            return jsonify({"success": False, "error": "Deal not found"}), 404

        data = request.json
        user_message = data.get("message")

        if not user_message:
            return jsonify({"success": False, "error": "Message required"}), 400

        # Start chat session if not already started
        start_chat_session(deal_id)

        # Send message
        result = send_chat_message(deal_id, user_message)

        return jsonify(result)

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/diligence/deals/<deal_id>/chat", methods=["GET"])
def api_get_chat_history(deal_id):
    """Get chat history"""
    try:
        if not validate_deal_id(deal_id):
            return jsonify({"success": False, "error": "Deal not found"}), 404

        history = get_chat_history(deal_id)

        return jsonify({
            "success": True,
            "messages": history
        })

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/diligence/deals/<deal_id>/chat", methods=["DELETE"])
def api_clear_chat(deal_id):
    """Clear chat history"""
    try:
        if not validate_deal_id(deal_id):
            return jsonify({"success": False, "error": "Deal not found"}), 404

        success = clear_chat_history(deal_id)

        if success:
            return jsonify({
                "success": True,
                "message": "Chat history cleared"
            })
        else:
            return jsonify({"success": False, "error": "Failed to clear chat"}), 500

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/diligence/deals/<deal_id>/link", methods=["POST"])
def api_link_to_crm(deal_id):
    """
    Link deal to CRM lead
    Body: {"lead_id": 123}
    """
    try:
        if not validate_deal_id(deal_id):
            return jsonify({"success": False, "error": "Deal not found"}), 404

        data = request.json
        lead_id = data.get("lead_id")

        if not lead_id:
            return jsonify({"success": False, "error": "lead_id required"}), 400

        success = link_deal_to_lead(deal_id, lead_id)

        if success:
            return jsonify({
                "success": True,
                "message": f"Linked deal to lead {lead_id}"
            })
        else:
            return jsonify({"success": False, "error": "Failed to link deal"}), 500

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/diligence/leads/<int:lead_id>/deals", methods=["GET"])
def api_get_lead_deals(lead_id):
    """Get all deals for a CRM lead"""
    try:
        deals = get_deals_for_lead(lead_id)

        return jsonify({
            "success": True,
            "lead_id": lead_id,
            "deals": deals
        })

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


# =============================================================================
# WEB PAGES
# =============================================================================

@app.route("/diligence")
def diligence_index():
    """Serve deal diligence UI"""
    return send_from_directory(app.static_folder, 'diligence.html')


@app.route("/diligence/deal/<deal_id>")
def diligence_deal_page(deal_id):
    """Serve single deal page"""
    # For now, redirect to main page with deal_id as query param
    # Frontend will handle loading the specific deal
    return send_from_directory(app.static_folder, 'diligence.html')


@app.route("/")
def index():
    """Root redirect"""
    return """
    <html>
    <head><title>MHP Diligence Agent</title></head>
    <body style="font-family: Arial, sans-serif; max-width: 800px; margin: 50px auto; padding: 20px;">
        <h1>MHP Deal Diligence Agent</h1>
        <p>Welcome to the Deal Diligence Agent for MHP acquisitions.</p>
        <p><a href="/diligence">Go to Diligence Dashboard →</a></p>
        <hr>
        <h2>Quick Start</h2>
        <ol>
            <li>Create a new deal</li>
            <li>Upload documents (Offering Memos, Financial Statements, etc.)</li>
            <li>Run AI analysis to generate investment report</li>
            <li>Chat with AI to ask questions about the deal</li>
            <li>(Optional) Link to CRM lead record</li>
        </ol>
        <hr>
        <p><small>Server running on port 8001 | <a href="/api/diligence/deals">API Docs</a></small></p>
    </body>
    </html>
    """


# =============================================================================
# ERROR HANDLERS
# =============================================================================

@app.errorhandler(404)
def not_found(error):
    return jsonify({"success": False, "error": "Endpoint not found"}), 404


@app.errorhandler(500)
def server_error(error):
    return jsonify({"success": False, "error": "Internal server error"}), 500


# =============================================================================
# MAIN
# =============================================================================

if __name__ == "__main__":
    print("=" * 60)
    print("Starting Deal Diligence Agent Server")
    print("=" * 60)
    print()
    print("API Endpoints:")
    print("  GET    /api/diligence/deals - List all deals")
    print("  POST   /api/diligence/deals - Create new deal")
    print("  GET    /api/diligence/deals/<id> - Get deal details")
    print("  POST   /api/diligence/deals/<id>/documents - Upload document")
    print("  POST   /api/diligence/deals/<id>/analyze - Run AI analysis")
    print("  GET    /api/diligence/deals/<id>/report - Get markdown report")
    print("  POST   /api/diligence/deals/<id>/chat - Send chat message")
    print("  GET    /api/diligence/deals/<id>/chat - Get chat history")
    print()
    print("Web Interface:")
    print("  http://localhost:8001/diligence")
    print()
    print("=" * 60)

    app.run(debug=True, port=8001, host='0.0.0.0')
