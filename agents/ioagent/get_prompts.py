import sys
from pathlib import Path
# Add project root to sys.path to allow importing from SalesforceMCP
root_dir = Path(__file__).resolve().parents[2]
if str(root_dir) not in sys.path:
    sys.path.append(str(root_dir))

#from mcp_module.Salesforcemcp.client.sf_client import SalesforceClient
import json
import logging

logger = logging.getLogger(__name__)

def convert_salesforce_json(json_data):
    """
    Converts Salesforce PromptTemplate JSON to the target dictionary format:
    {
      PromptTemplateName: [
        TemplateText, 
        LLMProvider, 
        LlmModel, 
        {ConfigName: DefaultValue}
      ]
    }
    """
    # Parse JSON if it's a string, otherwise assume it's already a list/dict
    if isinstance(json_data, str):
        data = json.loads(json_data)
    else:
        # If it's a dict and has 'records', use that list
        if isinstance(json_data, dict) and "records" in json_data:
            data = json_data["records"]
        else:
            data = json_data

    output_dict = {}

    for entry in data:
        # 1. Extract the Key (PromptTemplate__r.Name)
        template_info = entry.get("PromptTemplate__r", {})
        template_name = template_info.get("Name")

        # Skip entries that don't have a template name
        if not template_name:
            continue

        # 2. Extract Top Level Fields
        template_text = entry.get("TemplateText__c", "")
        llm_provider = entry.get("LLMProvider__c", "")
        llm_model = entry.get("LlmModel__c", "")

        # 3. Extract and Build PromptConfigs Dictionary
        configs_map = {}
        prompt_configs = entry.get("PromptConfigs__r")

        # Check if PromptConfigs exists and has records
        if prompt_configs and isinstance(prompt_configs, dict):
            records = prompt_configs.get("records", [])
            if records:
                for record in records:
                    conf_name = record.get("Name")
                    # Note: The provided JSON example does not contain 'DefaultValue__c',
                    # so this will return None unless the field exists in the source.
                    conf_value = record.get("DefaultValue__c")
                    
                    if conf_name:
                        configs_map[conf_name] = conf_value

        # 4. Construct the list for this key
        output_dict[template_name] = [
            template_text,
            llm_provider,
            llm_model,
            configs_map
        ]

    return output_dict

def fetch_prompts():
    try:
        sf_client = SalesforceClient("agent")
        sf_client.connect()
        sf = sf_client.sf

        md_query = f"""
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
        logger.error(f"Error fetching prompts: {e}")
        return {}


if __name__ == "__main__":
    output = fetch_prompts()
    print(json.dumps(output, indent=2))
