-- SQLite schema for MHP/RV Park CRM
-- Database: data/leads.db

-- Leads table: all scraped properties (deduplicated by place_id)
CREATE TABLE IF NOT EXISTS leads (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    place_id TEXT UNIQUE NOT NULL,
    crexi_id TEXT,
    loopnet_id TEXT,
    
    -- Basic info from scrape
    name TEXT NOT NULL,
    address TEXT,
    city TEXT,
    state TEXT,
    zip TEXT,
    phone TEXT,
    website TEXT,
    maps_url TEXT,
    latitude REAL,
    longitude REAL,
    
    -- Google data
    google_rating REAL,
    review_count INTEGER,
    category TEXT,
    
    -- Social / Enrichment
    social_facebook TEXT,
    social_instagram TEXT,
    social_linkedin TEXT,
    is_enriched INTEGER DEFAULT 0,
    registered_agent_name TEXT,
    registered_agent_address TEXT,
    utilities_status TEXT,
    rent_info TEXT,
    
    -- Scoring
    site_score_1_10 INTEGER DEFAULT 0,
    score_breakdown_json TEXT,
    score_reasons TEXT,
    crawl_status TEXT,
    
    -- Owner Fatigue Score (acquisition targeting)
    owner_fatigue_score_0_100 REAL,
    owner_fatigue_confidence TEXT,  -- 'high' | 'medium' | 'low'
    owner_fatigue_reasons_json TEXT,
    owner_fatigue_breakdown_json TEXT,
    
    -- Scrape metadata
    source_query TEXT,
    loopnet_url TEXT,
    listing_url TEXT,
    scrape_source TEXT,
    area TEXT,
    zcta_derived TEXT,
    first_scraped_at TEXT NOT NULL,
    last_scraped_at TEXT NOT NULL,
    
    -- Amenity Data (Points of Interest)
    nearest_supermarket_name TEXT,
    nearest_supermarket_dist REAL,
    nearest_hospital_name TEXT,
    nearest_hospital_dist REAL,
    nearest_school_name TEXT,
    nearest_school_dist REAL,
    amenity_score INTEGER,
    
    -- CRM fields
    status TEXT DEFAULT 'not_contacted' CHECK (status IN (
        'not_contacted',
        'contacted',
        'interested',
        'not_interested',
        'docs_requested',
        'docs_received',
        'reviewed_interested',
        'reviewed_not_interested',
        'negotiating',
        'acquired',
        'dead'
    )),
    owner_name TEXT,
    owner_email TEXT,
    owner_phone TEXT,
    list_price REAL,
    broker_name TEXT,
    broker_firm TEXT,
    broker_company TEXT,
    asking_price REAL,
    cap_rate REAL,
    noi REAL,
    price_per_unit REAL,
    lot_count INTEGER,
    occupancy_rate REAL,
    occupancy REAL,
    is_manual_entry INTEGER DEFAULT 0,
    
    -- Expanded Property Details
    year_built INTEGER,
    building_size REAL,
    lot_size REAL,
    sq_ft REAL,
    description TEXT,
    detailed_description TEXT,
    description_keywords TEXT,
    listing_status TEXT,
    lease_type TEXT,
    tenancy TEXT,
    lease_expiration TEXT,
    sub_type TEXT,
    date_listed TEXT,
    days_on_market INTEGER,
    investment_highlights TEXT,
    
    next_followup DATE,
    
    -- Tax Shock Risk Score
    tax_shock_score_0_100 REAL,
    tax_shock_confidence TEXT,
    county_name TEXT,
    county_fips TEXT,
    county_millage_change REAL,
    county_taxes_levied_growth REAL,
    tax_shock_reasons_json TEXT,
    tax_shock_breakdown_json TEXT,

    -- Insurance / Risk Data
    insurance_pressure_score_0_100 REAL,
    insurance_pressure_confidence TEXT,
    flood_zone TEXT,
    storm_proximity_score REAL,
    disaster_pressure_score REAL,
    insurance_pressure_reasons_json TEXT,
    insurance_pressure_breakdown_json TEXT,

    -- Broker Info
    broker_name TEXT,
    broker_company TEXT,
    broker_phone TEXT,
    broker_email TEXT,
    listing_url TEXT,
    crexi_id TEXT,
    asking_price REAL,
    
    -- Aggregated stats (updated by triggers/code)
    call_count INTEGER DEFAULT 0,
    last_called_at TEXT,
    last_call_outcome TEXT,
    email_count INTEGER DEFAULT 0,
    last_email_at TEXT,

    -- Tags (comma-separated list)
    tags TEXT,

    archived INTEGER DEFAULT 0
);

-- County Tax Metrics (Florida DOR)
CREATE TABLE IF NOT EXISTS county_tax_metrics (
    county_fips TEXT,
    year INTEGER,
    total_millage REAL,
    taxes_levied REAL,
    yoy_millage_change REAL,
    yoy_taxes_growth REAL,
    computed_at TEXT,
    PRIMARY KEY (county_fips, year)
);

-- Index for fast lookups
CREATE INDEX IF NOT EXISTS idx_leads_status ON leads(status);
CREATE INDEX IF NOT EXISTS idx_leads_area ON leads(area);
CREATE INDEX IF NOT EXISTS idx_leads_score ON leads(site_score_1_10);

-- Call log table
CREATE TABLE IF NOT EXISTS calls (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    lead_id INTEGER NOT NULL,
    called_at TEXT NOT NULL,
    outcome TEXT CHECK (outcome IN (
        'no_answer',
        'left_voicemail',
        'spoke_with_owner',
        'spoke_with_manager',
        'wrong_number',
        'not_interested',
        'interested',
        'scheduled_callback',
        'other'
    )),
    duration_seconds INTEGER,
    notes TEXT,
    next_action TEXT,
    next_followup_date DATE,
    created_at TEXT DEFAULT (datetime('now')),
    
    FOREIGN KEY (lead_id) REFERENCES leads(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_calls_lead ON calls(lead_id);
CREATE INDEX IF NOT EXISTS idx_calls_date ON calls(called_at);

-- Email table (synced from Gmail)
CREATE TABLE IF NOT EXISTS emails (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    lead_id INTEGER NOT NULL,
    gmail_thread_id TEXT UNIQUE,
    gmail_message_id TEXT,
    direction TEXT CHECK (direction IN ('sent', 'received')),
    subject TEXT,
    snippet TEXT,
    from_address TEXT,
    to_address TEXT,
    email_date TEXT NOT NULL,
    is_read INTEGER DEFAULT 0,
    labels TEXT, -- JSON array of Gmail labels
    synced_at TEXT DEFAULT (datetime('now')),
    
    FOREIGN KEY (lead_id) REFERENCES leads(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_emails_lead ON emails(lead_id);
CREATE INDEX IF NOT EXISTS idx_emails_date ON emails(email_date);
CREATE INDEX IF NOT EXISTS idx_emails_thread ON emails(gmail_thread_id);

-- Notes table (free-form notes)
CREATE TABLE IF NOT EXISTS notes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    lead_id INTEGER NOT NULL,
    content TEXT NOT NULL,
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT,
    
    FOREIGN KEY (lead_id) REFERENCES leads(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_notes_lead ON notes(lead_id);

-- Activity log (unified timeline)
CREATE TABLE IF NOT EXISTS activity_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    lead_id INTEGER NOT NULL,
    activity_type TEXT CHECK (activity_type IN (
        'scraped',
        'status_change',
        'call',
        'email_sent',
        'email_received',
        'note_added',
        'followup_set'
    )),
    description TEXT,
    metadata_json TEXT, -- Additional context
    created_at TEXT DEFAULT (datetime('now')),
    
    FOREIGN KEY (lead_id) REFERENCES leads(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_activity_lead ON activity_log(lead_id);
CREATE INDEX IF NOT EXISTS idx_activity_type ON activity_log(activity_type);

-- Landwatch Scraper Data (Raw JSON dump)
CREATE TABLE IF NOT EXISTS landwatch_raw_listings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    landwatch_id INTEGER UNIQUE, -- Extracted from ID field in JSON
    json_data TEXT NOT NULL,     -- Full JSON dump
    scraped_at TEXT DEFAULT (datetime('now')),
    processed_at TEXT            -- When it was merged into leads table
);

CREATE INDEX IF NOT EXISTS idx_landwatch_raw_id ON landwatch_raw_listings(landwatch_id);
