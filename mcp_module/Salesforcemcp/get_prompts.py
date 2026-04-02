
import logging
import json
from .client.sf_client import SalesforceClient

logger = logging.getLogger(__name__)

def convert_salesforce_json(json_data):
    if isinstance(json_data, str):
        data = json.loads(json_data)
    else:
        if isinstance(json_data, dict) and "records" in json_data:
            data = json_data["records"]
        else:
            data = json_data

    output_dict = {}

    for entry in data:
        template_info = entry.get("PromptTemplate__r", {})
        template_name = template_info.get("Name")
        if not template_name:
            continue

        template_text = entry.get("TemplateText__c", "")
        llm_provider = entry.get("LLMProvider__c", "")
        llm_model = entry.get("LlmModel__c", "")

        configs_map = {}
        prompt_configs = entry.get("PromptConfigs__r")

        if prompt_configs and isinstance(prompt_configs, dict):
            records = prompt_configs.get("records", [])
            for record in records:
                conf_name = record.get("Name")
                conf_value = record.get("DefaultValue__c")
                if conf_name:
                    configs_map[conf_name] = conf_value

        output_dict[template_name] = [
            template_text,
            llm_provider,
            llm_model,
            configs_map
        ]
    return output_dict

def fetch_prompts(org_type="agent"):
    try:
        sf_client = SalesforceClient(org_type)
        if not sf_client.connect():
            logger.warning(f"Salesforce client ({org_type}) not connected. Skipping prompt fetch.")
            return {}

        sf = sf_client.sf
        md_query = """
            SELECT
        PromptTemplate__r.Name,
        PromptTemplate__r.Description__c,
        Name,
        VersionNumber__c,
        TemplateText__c,
        LLMProvider__c,
        LLMModel__c,
        (
            SELECT Name, ConfigType__c, PlaceholderName__c, Description__c
            FROM PromptConfigs__r
        )
        FROM PromptTemplateVersion__c
        WHERE Status__c = 'Active'
        """
        md_result = sf.query(md_query)
        return convert_salesforce_json(md_result)
    except Exception as e:
        logger.error(f"Error fetching prompts from {org_type}: {e}")
        return {}
