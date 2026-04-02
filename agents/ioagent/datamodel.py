from typing import List, Optional, Any, Dict
from pydantic import BaseModel, Field

# --- Data Models ---

class MediaCompany(BaseModel):
    name: Optional[str] = None
    address: Optional[str] = None
    billing_contact: Optional[str] = None
    billing_email: Optional[str] = None
    ein: Optional[str] = None

class ClientAgency(BaseModel):
    name: Optional[str] = None
    type: Optional[str] = None
    address: Optional[str] = None
    billing_address: Optional[dict] = None
    shipping_address: Optional[dict] = None
    contact_name: Optional[str] = None
    contact_email: Optional[str] = None
    contact_phone: Optional[str] = None
    billing_contact: Optional[str] = None
    billing_email: Optional[str] = None
    ein: Optional[str] = None
    taxation_number: Optional[str] = None
    company_number: Optional[str] = None
    vat_registration_number: Optional[str] = None

class Terms(BaseModel):
    billing_data: Optional[str] = None
    payment_term: Optional[str] = None
    currency: Optional[str] = None
    geos: Optional[str] = None
    rate: Optional[str] = None
    impressions: Optional[str] = None
    formats: Optional[str] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    additional_requirements: Optional[str] = None

class LineItem(BaseModel):
    id: Optional[str] = None
    name: Optional[str] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    budget: Optional[float] = None
    objective: Optional[str] = None
    pacing: Optional[str] = None
    impressions: Optional[str] = None
    rate: Optional[str] = None


class CampaignInformation(BaseModel):
    campaign_name: Optional[str] = None
    campaign_start_date: Optional[str] = None
    campaign_end_date: Optional[str] = None

class FinalizedRecord(BaseModel):
    opportunity: Optional[dict] = None
    account: Optional[dict] = None
    quote_id: Optional[str] = None
    quote_name: Optional[str] = None
    pricebook_id: Optional[str] = None

# --- State ---

class IOState(BaseModel):
    # Raw input
    io_markdown: Optional[str] = None
    case_id: Optional[str] = None
    content_document_id: Optional[str] = None
    attachments_list: List[Dict[str, Any]] = Field(default_factory=list)
    awaiting_file_selection: bool = False
    error_message: Optional[str] = None
    error_flag: bool = False
    mcp_response: Optional[dict] = None
    # Header extraction results
    media_company: Optional[MediaCompany] = None
    client_agency: Optional[ClientAgency] = None
    campaign_information: Optional[CampaignInformation] = None
    terms: Optional[Terms] = None
    io_id: Optional[str] = None
    order_id: Optional[str] = None

    # Line item extraction results
    line_items: List[LineItem] = Field(default_factory=list)

    # Validation flags
    header_valid: bool = False
    line_items_valid: bool = False
    header_errors: List[str] = Field(default_factory=list)
    line_errors: List[str] = Field(default_factory=list)

    # Retry counters
    header_attempt: int = 0
    line_attempt: int = 0
    max_attempts: int = 3

    #temp variables
    temp_similarity_inputdata: Optional[list] = None
    temp_similarity_soql_data: Optional[str] = None

    # Opportunity Matching
    matched_opportunity_records: List[list] = Field(default_factory=list) # [[record, score], ...]
    matched_opportunity_type: Optional[str] = None

    # Account Matching
    temp_account_soql_data: Optional[str] = None
    temp_account_similarity_inputdata: Optional[list] = None
    matched_account_records: List[list] = Field(default_factory=list)
    matched_account_type: Optional[str] = None

    # Quote Matching
    quote_soql: Optional[str] = None
    quote_mapping_json: Optional[list] = None
    matched_quote_line_items: List[Any] = Field(default_factory=list)
    best_matched_line_items: List[Any] = Field(default_factory=list)
    failed_line_items: List[Any] = Field(default_factory=list)
    created_line_items: List[Any] = Field(default_factory=list)
    matched_quote_type: Optional[str] = None

    # Finalized Record
    finalized_record: Optional[FinalizedRecord] = None

    # Salesforce Payload
    salesforce_payload: Optional[dict] = None
    
    # Payload fields
    order_payload_json: Optional[Dict[str, Any]] = None
    order_items_payload_json: Optional[List[Dict[str, Any]]] = None
    insertion_errors: Optional[List[str]] = None
    
    # User Intent
    user_input: Optional[str] = None
    intent_valid: bool = False
    agent_response: Optional[str] = None  

    # Final Result
    final_result: Optional[dict] = None

    #flags
    strict_validation_for_lineitems:bool= True
    
    # Prompts
    dict_of_prompts: Optional[dict] = None

    #data raptor 
    data_wrap: Optional[Dict[Any, Any]] = None

    # Selection Flags
    awaiting_selection: bool = False
    user_selection: Optional[str] = None

class prompts(BaseModel):
    dict_of_prompts: Optional[dict] = None
    

