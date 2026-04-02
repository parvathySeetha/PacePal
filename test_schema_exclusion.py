import json
import os

def get_excluded_objects_from_schema():
    """
    Reads schema_metadata.json to return a list of objects that should be excluded from chat links.
    """
    try:
        # Assuming run from project root
        project_root = os.getcwd() 
        schema_path = os.path.join(project_root, "schema_metadata.json")
        
        if not os.path.exists(schema_path):
            print(f"❌ Schema file not found at {schema_path}")
            return []
        
        with open(schema_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        
        excluded = []
        if isinstance(data, list):
            for item in data:
                if item.get("exclude_from_chat_links"):
                    excluded.append(item.get("object"))
        return excluded
    except Exception as e:
        print(f"❌ Error: {e}")
        return []

if __name__ == "__main__":
    excluded = get_excluded_objects_from_schema()
    print(f"Excluded objects: {excluded}")
    if "CampaignMember" in excluded:
        print("✅ SUCCESS: CampaignMember is excluded.")
    else:
        print("❌ FAILURE: CampaignMember is NOT excluded.")
