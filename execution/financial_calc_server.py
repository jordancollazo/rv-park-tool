"""
Financial Calculator Flask Server
Stages 2 & 3: Data validation form + Interactive calculator
"""

import os
import json
import uuid
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List

from flask import Flask, request, jsonify, send_from_directory, render_template_string
from werkzeug.utils import secure_filename
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

# Set API key (fallback if not in .env)
if not os.getenv('ANTHROPIC_API_KEY'):
    raise ValueError("ANTHROPIC_API_KEY not found in environment. Please set it in .env file.")

# Import Stage 1 OCR and web scraping functions
from financial_calc_ocr import process_listing_screenshot
from financial_calc_webscrape import process_listing_url

app = Flask(__name__)

# Use absolute paths
BASE_DIR = Path(__file__).resolve().parent.parent
CALCULATOR_DIR = BASE_DIR / '.tmp' / 'calculator_option_1'

app.config['UPLOAD_FOLDER'] = str(CALCULATOR_DIR / 'uploads')
app.config['LEADS_FOLDER'] = str(CALCULATOR_DIR / 'leads')
app.config['SCENARIOS_FOLDER'] = str(CALCULATOR_DIR / 'scenarios')
app.config['STATIC_FOLDER'] = str(CALCULATOR_DIR / 'static')
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max upload

ALLOWED_EXTENSIONS = {'png', 'jpg', 'jpeg', 'pdf'}

# Ensure directories exist
for folder in [app.config['UPLOAD_FOLDER'], app.config['LEADS_FOLDER'], app.config['SCENARIOS_FOLDER']]:
    os.makedirs(folder, exist_ok=True)


def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


# ==================== STAGE 1: OCR UPLOAD ====================

@app.route('/api/upload', methods=['POST'])
def upload_screenshot():
    """Upload screenshot and run OCR + LLM parsing"""
    if 'file' not in request.files:
        return jsonify({'error': 'No file uploaded'}), 400

    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No file selected'}), 400

    if not allowed_file(file.filename):
        return jsonify({'error': 'Invalid file type. Use PNG, JPG, or PDF'}), 400

    # Save uploaded file
    filename = secure_filename(file.filename)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    unique_filename = f"{timestamp}_{filename}"
    filepath = os.path.join(app.config['UPLOAD_FOLDER'], unique_filename)
    file.save(filepath)

    try:
        # Run OCR + LLM parsing
        result = process_listing_screenshot(filepath, app.config['LEADS_FOLDER'])
        return jsonify({
            'success': True,
            'lead_id': result['lead_id'],
            'data': result
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/scrape', methods=['POST'])
def scrape_url():
    """Scrape property listing from URL"""
    data = request.json

    if not data or 'url' not in data:
        return jsonify({'error': 'URL is required'}), 400

    url = data['url'].strip()

    if not url.startswith('http'):
        return jsonify({'error': 'Invalid URL. Must start with http:// or https://'}), 400

    try:
        # Run web scraping + LLM parsing
        result = process_listing_url(url, app.config['LEADS_FOLDER'])
        return jsonify({
            'success': True,
            'lead_id': result['lead_id'],
            'data': result
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/parse_text', methods=['POST'])
def parse_text():
    """Parse pasted listing text with LLM"""
    from financial_calc_webscrape import parse_listing_with_llm

    data = request.json

    if not data or 'text' not in data:
        return jsonify({'error': 'Text is required'}), 400

    text = data['text'].strip()

    if not text:
        return jsonify({'error': 'Text cannot be empty'}), 400

    try:
        # Generate lead_id
        lead_id = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Parse with LLM
        parsed_data = parse_listing_with_llm(text, "manual_text_input")

        # Add metadata
        parsed_data["source_text"] = text[:5000]  # Save first 5000 chars
        parsed_data["processed_at"] = datetime.now().isoformat()
        parsed_data["lead_id"] = lead_id

        # Save to JSON
        os.makedirs(app.config['LEADS_FOLDER'], exist_ok=True)
        output_path = os.path.join(app.config['LEADS_FOLDER'], f"{lead_id}.json")

        with open(output_path, 'w') as f:
            json.dump(parsed_data, f, indent=2)

        return jsonify({
            'success': True,
            'lead_id': lead_id,
            'data': parsed_data
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


# ==================== STAGE 2: DATA VALIDATION ====================

@app.route('/api/lead/<lead_id>', methods=['GET'])
def get_lead(lead_id):
    """Retrieve parsed lead data"""
    lead_path = os.path.join(app.config['LEADS_FOLDER'], f"{lead_id}.json")

    if not os.path.exists(lead_path):
        return jsonify({'error': 'Lead not found'}), 404

    with open(lead_path, 'r') as f:
        data = json.load(f)

    return jsonify(data)


@app.route('/api/lead/<lead_id>', methods=['PUT'])
def update_lead(lead_id):
    """Update lead data after manual corrections"""
    lead_path = os.path.join(app.config['LEADS_FOLDER'], f"{lead_id}.json")

    if not os.path.exists(lead_path):
        return jsonify({'error': 'Lead not found'}), 404

    # Load existing data
    with open(lead_path, 'r') as f:
        existing = json.load(f)

    # Update with new data
    updated_data = request.json
    existing.update(updated_data)
    existing['updated_at'] = datetime.now().isoformat()

    # Validate required fields
    errors = []
    if not existing.get('property_name'):
        errors.append('property_name is required')
    if not existing.get('purchase_price'):
        errors.append('purchase_price is required')

    if errors:
        return jsonify({'error': 'Validation failed', 'details': errors}), 400

    # Save updated data
    with open(lead_path, 'w') as f:
        json.dump(existing, f, indent=2)

    return jsonify({'success': True, 'data': existing})


# ==================== STAGE 3: FINANCIAL CALCULATIONS ====================

def calculate_monthly_payment(principal: float, annual_rate: float, months: int) -> float:
    """Calculate monthly P&I payment"""
    if annual_rate == 0:
        return principal / months if months > 0 else 0

    monthly_rate = annual_rate / 100 / 12
    payment = principal * (monthly_rate * (1 + monthly_rate)**months) / ((1 + monthly_rate)**months - 1)
    return payment


def generate_amortization_schedule(principal: float, annual_rate: float, months: int,
                                   interest_only_months: int = 0) -> List[Dict[str, float]]:
    """Generate month-by-month amortization schedule"""
    schedule = []
    balance = principal
    monthly_rate = annual_rate / 100 / 12

    # Interest-only period
    for month in range(1, interest_only_months + 1):
        interest = balance * monthly_rate
        schedule.append({
            'month': month,
            'payment': interest,
            'principal': 0,
            'interest': interest,
            'balance': balance
        })

    # Amortizing period
    remaining_months = months - interest_only_months
    if remaining_months > 0:
        payment = calculate_monthly_payment(balance, annual_rate, remaining_months)

        for month in range(interest_only_months + 1, months + 1):
            interest = balance * monthly_rate
            principal_pay = payment - interest
            balance -= principal_pay

            schedule.append({
                'month': month,
                'payment': payment,
                'principal': principal_pay,
                'interest': interest,
                'balance': max(0, balance)
            })

    return schedule


@app.route('/api/calculate/financing', methods=['POST'])
def calculate_financing():
    """Calculate financing structure"""
    data = request.json

    purchase_price = float(data['purchase_price'])
    down_payment_pct = float(data['down_payment_pct'])
    interest_rate = float(data['interest_rate'])
    amortization_months = int(data['amortization_months'])
    balloon_years = data.get('balloon_years')
    interest_only_months = int(data.get('interest_only_months', 0))

    # Calculate loan amounts
    down_payment = purchase_price * (down_payment_pct / 100)
    loan_amount = purchase_price - down_payment

    # Generate amortization schedule
    schedule = generate_amortization_schedule(
        loan_amount, interest_rate, amortization_months, interest_only_months
    )

    # Calculate totals
    total_interest = sum(p['interest'] for p in schedule)
    monthly_payment = schedule[0]['payment'] if schedule else 0

    # Balloon payment
    balloon_payment = 0
    if balloon_years:
        balloon_month = int(balloon_years) * 12
        if balloon_month < len(schedule):
            balloon_payment = schedule[balloon_month - 1]['balance']

    return jsonify({
        'down_payment': down_payment,
        'loan_amount': loan_amount,
        'monthly_payment': monthly_payment,
        'total_interest': total_interest,
        'balloon_payment': balloon_payment,
        'schedule': schedule[:360]  # Limit to 30 years for response size
    })


@app.route('/api/calculate/hard_money', methods=['POST'])
def calculate_hard_money():
    """Calculate hard money loan payments"""
    data = request.json

    amount = float(data['amount'])
    interest_rate = float(data['interest_rate'])
    term_months = int(data['term_months'])

    schedule = generate_amortization_schedule(amount, interest_rate, term_months)

    return jsonify({
        'monthly_payment': schedule[0]['payment'] if schedule else 0,
        'total_interest': sum(p['interest'] for p in schedule),
        'schedule': schedule
    })


@app.route('/api/calculate/lp_waterfall', methods=['POST'])
def calculate_lp_waterfall():
    """Calculate LP investment waterfall distributions"""
    data = request.json

    # Property financials
    purchase_price = float(data['purchase_price'])
    noi = float(data.get('noi', 0))
    monthly_income = noi / 12 if noi else 0

    # Debt service
    primary_debt_payment = float(data.get('primary_debt_payment', 0))
    hard_money_payment = float(data.get('hard_money_payment', 0))
    total_debt_service = primary_debt_payment + hard_money_payment

    # LP investors
    investors = data.get('investors', [])
    total_lp_investment = sum(float(inv['amount']) for inv in investors)

    # Calculate ownership percentages
    for investor in investors:
        investor['ownership_pct'] = (float(investor['amount']) / total_lp_investment * 100) if total_lp_investment else 0

    # Monthly cash flow
    monthly_cash_flow = monthly_income - total_debt_service

    # Distribute to LPs pro-rata
    for investor in investors:
        investor['monthly_distribution'] = monthly_cash_flow * (investor['ownership_pct'] / 100)

    # 12-month projection
    waterfall = []
    for month in range(1, 13):
        month_data = {
            'month': month,
            'income': monthly_income,
            'debt_service': total_debt_service,
            'cash_flow': monthly_cash_flow,
            'distributions': []
        }

        for investor in investors:
            month_data['distributions'].append({
                'name': investor['name'],
                'amount': investor['monthly_distribution'],
                'cumulative': investor['monthly_distribution'] * month
            })

        waterfall.append(month_data)

    return jsonify({
        'total_lp_investment': total_lp_investment,
        'monthly_cash_flow': monthly_cash_flow,
        'annual_cash_flow': monthly_cash_flow * 12,
        'investors': investors,
        'waterfall': waterfall
    })


@app.route('/api/scenario/save', methods=['POST'])
def save_scenario():
    """Save calculator scenario"""
    data = request.json

    scenario_id = str(uuid.uuid4())
    scenario = {
        'scenario_id': scenario_id,
        'lead_id': data.get('lead_id'),
        'created_at': datetime.now().isoformat(),
        'financing': data.get('financing'),
        'investors': data.get('investors'),
        'calculations': data.get('calculations')
    }

    scenario_path = os.path.join(app.config['SCENARIOS_FOLDER'], f"{scenario_id}.json")
    with open(scenario_path, 'w') as f:
        json.dump(scenario, f, indent=2)

    return jsonify({'success': True, 'scenario_id': scenario_id})


@app.route('/api/scenario/<scenario_id>', methods=['GET'])
def get_scenario(scenario_id):
    """Retrieve saved scenario"""
    scenario_path = os.path.join(app.config['SCENARIOS_FOLDER'], f"{scenario_id}.json")

    if not os.path.exists(scenario_path):
        return jsonify({'error': 'Scenario not found'}), 404

    with open(scenario_path, 'r') as f:
        data = json.load(f)

    return jsonify(data)


# ==================== FRONTEND ====================

@app.route('/')
def index():
    """Serve main calculator page"""
    return send_from_directory(app.config['STATIC_FOLDER'], 'calculator.html')


@app.route('/calculator.js')
def serve_js():
    """Serve calculator JavaScript"""
    return send_from_directory(app.config['STATIC_FOLDER'], 'calculator.js')


@app.route('/static/<path:filename>')
def serve_static(filename):
    """Serve static files"""
    return send_from_directory(app.config['STATIC_FOLDER'], filename)


# ==================== RUN SERVER ====================

if __name__ == '__main__':
    print("="*60)
    print("Financial Calculator Server")
    print("="*60)
    print("Starting server at http://localhost:5000")
    print("Upload screenshots to begin OCR parsing")
    print("="*60)

    app.run(debug=True, host='0.0.0.0', port=5000)
