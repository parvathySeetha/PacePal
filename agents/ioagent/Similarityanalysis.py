from email._header_value_parser import Address
import json
import logging
import rapidfuzz as fuzz
import sys
from pathlib import Path
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

# Configure logger (inherits from root logger configured in nodes.py)
logger = logging.getLogger(__name__)

class similarutyanalysistool:
    def __init__(self):
        pass

    async def getSoqlData(self, Soql):
        soqldata = None
        Outputdict={}
        
        # mcp_module lives in the project root
        ioagent_dir = Path(__file__).resolve().parent
        PROJECT_ROOT = ioagent_dir.parent.parent
        server_path = PROJECT_ROOT / "mcp_module" / "Salesforcemcp" / "sf_server.py"

        server_params = StdioServerParameters(
            command=sys.executable,
            args=[str(server_path)],
        )

        print("🔌 Connecting to server...")
        logger.info("🔌 Connecting to server...")

        try:
            async with stdio_client(server_params) as (read, write):
                async with ClientSession(read, write) as session:
                    
                    await session.initialize()
                    print("✅ Server initialized successfully!")
                    logger.info("✅ Server initialized successfully!")
                    # List tools
                    # tools = await session.list_tools()
                    
                    # for tool in tools.tools:
                    #     print("\n=== TOOL ===")
                    #     print("Name:", tool.name)
                    #     print("Description:", tool.description)
                    #     print("Schema:", tool.inputSchema)
                    
                    """--------------------------------"""
                    SoqlData = await session.call_tool("run_dynamic_soql", arguments={"query": Soql}) 
                    print("SoqlData",SoqlData)
                    logger.info(f"SoqlData: {SoqlData}")
                    
                    #print(json.dumps(SoqlData.model_dump(), indent=2))
                   # print((SoqlData.model_dump().values()))
                    # for i in SoqlData.model_dump().values():
                    #     print(i,'\n')
                    #     print(type(i))

                # Parse the JSON string from the text content
                records = json.loads(SoqlData.content[0].text)
                #print("records1234",records)
                # print(records['records'])
                for record in records['records']:
                # print(f"Account Details: {record}")
                # print(type(record))
                # print(record['Name'])
                # print(record['Name'])
                    # print("delat_apha")
                    tempdict={}
                    for key, value in record.items():
                        
                        
                        #print(f"{key}: {value}")
                        tempdict[key] = value
                    #Outputdict.append(tempdict)
                #print(tempdict['Id'])
                #print("\n")
                    Outputdict[tempdict['Id']]=tempdict
                    #print("Outputdict1234",Outputdict)
                logger.info(f"Outputdict_similarity: {Outputdict}")
                return Outputdict

                           
        except Exception as e:
            print(f"\n❌ Error: {e}")
            logger.error(f"❌ Error in getSoqlData: {e}")

        # if not SoqlData.isError:
        #     # Parse the JSON string from the text content
        #     records = json.loads(SoqlData.content[0].text)
        #     # print(records)
        #     # print(records['records'])

        #     for record in records['records']:
        #         # print(f"Account Details: {record}")
        #         # print(type(record))
        #         # print(record['Name'])
        #         tempdict={}
        #         for key, value in record.items():
                    
        #             #print(f"{key}: {value}")
        #             tempdict[key] = value
        #         #Outputdict.append(tempdict)
        #         #print(tempdict['Id'])
        #         #print("\n")
        #         Outputdict[tempdict['Id']]=tempdict
        #     #print(Outputdict)
            #     Outputdict[tempdict['Id']]=tempdict
        #     #print(Outputdict)
        #     #return Outputdict

    def _flatten_json(self, nested_json, parent_key='', sep='.'):
        """
        Flatten a nested dictionary.
        """
        items = []
        for k, v in nested_json.items():
            new_key = parent_key + sep + k if parent_key else k
            if isinstance(v, dict):
                items.extend(self._flatten_json(v, new_key, sep=sep).items())
            else:
                items.append((new_key, v))
        return dict(items)

    def preprocess_records(self, records):
        """
        Flattens records if they contain nested JSON.
        """
        processed_records = {}
        if not records:
            return processed_records
            
        for record_id, record in records.items():
            # Check if any value is a dict (nested)
            if any(isinstance(v, dict) for v in record.values()):
                processed_records[record_id] = self._flatten_json(record)
            else:
                processed_records[record_id] = record
        return processed_records

    def inputmapper(self, records, inputdata):
        output_dict = {}
        if not records:
            return output_dict
            
        for record_id, record in records.items():
            mapped_items = []
            for item in inputdata:
                # Ensure item has at least name and fields
                if len(item) < 2:
                    continue
                
                name = item[0]
                fields = item[1]
                score = item[2] if len(item) > 2 else None
                
                resolved_values = []
                for field_api in fields:
                    # Handle dot notation for nested fields
                    # Check direct access first (for flattened records)
                    if field_api in record:
                        resolved_values.append(record[field_api])
                        continue

                    value = record
                    try:
                        for part in field_api.split('.'):
                            if isinstance(value, dict) and part in value:
                                value = value[part]
                            else:
                                value = None
                                break
                        resolved_values.append(value)
                    except Exception:
                        resolved_values.append(None)
                
                mapped_items.append([name, resolved_values, score])
            
            output_dict[record_id] = mapped_items
        
        #print(json.dumps(output_dict, indent=2))
        #print("output_dict",output_dict)
        return output_dict

    def analysis(self, seasoneddata):
        final_scores = {}
        
        for record_id, items in seasoneddata.items():
            total_weighted_score = 0
            total_weight = 0
            
            for item in items:
                # item structure: [target_value, [sf_values], weight]
                if len(item) < 3:
                    continue
                logger.info(f"item for comp. similarity {item}")        
                target_value = str(item[0]) if item[0] is not None else ""
                sf_values = item[1]
                weight = item[2] if item[2] is not None else 0
                
                if weight == 0:
                    continue
                    
                # Filter out None values
                valid_sf_values = [v for v in sf_values if v is not None]
                
                if not valid_sf_values:
                    logger.info(f"Skipping comparison for {target_value} - SF value is None/Empty")
                    continue
               

                total_weight += weight
                
                best_match_ratio = 0
                for val in valid_sf_values:
                    val_str = str(val)
                    # Calculate ratio
                    ratio = fuzz.fuzz.ratio(target_value.lower(), val_str.lower())
                    logger.info(f"ratio for each field {ratio} /n {target_value} -- {val_str} -- {weight}")
                    if ratio > best_match_ratio:
                        best_match_ratio = ratio
                
                # Calculate inputdata score (weighted)
                inputdata_score = best_match_ratio * weight
                logger.info(f"inputdata_score {inputdata_score} /n {best_match_ratio} -- {weight}")
                total_weighted_score += inputdata_score
            
            # Normalize final score to 0-100
            if total_weight > 0:
                normalized_score = total_weighted_score / total_weight
            else:
                normalized_score = 0
                
            final_scores[record_id] = normalized_score
            
        return final_scores



    def outputmapper(self, seasoneddata, records):
        # seasoneddata: {<id>: final_score}
        # records: {<id>: <recorddata>}
        output_data = {}
        
        for record_id, score in seasoneddata.items():
            if record_id in records:
                record_data = records[record_id]
                # Format: {<id>: [[<recorddata>], score]}
                output_data[record_id] = [[record_data], score]
        
        #print(json.dumps(output_data, indent=2))
        return output_data


    def run(self, records, inputdata):
        """
        Run analysis using pre-fetched records.
        """
        # Preprocess (flatten) data for analysis
        flat_data = self.preprocess_records(records)
        seasoned_data = self.inputmapper(flat_data, inputdata)                           
        scores = self.analysis(seasoned_data)
        outputrdataFinal = self.outputmapper(scores, records)
        return outputrdataFinal

def sort_output(output_data):
    # output_data format: {<id>: [[<recorddata>], score]}
    # Sort by score (index 1 of the value) in descending order
    sorted_items = sorted(output_data.items(), key=lambda item: item[1][1], reverse=True)
    # Convert back to dictionary (preserving order)
    return dict(sorted_items)

def run_similarity_analysis(records, inputdata):
    tool = similarutyanalysistool()
    result = tool.run(records, inputdata)
    return sort_output(result)

if __name__ == "__main__":
    tool = similarutyanalysistool()
    
    # Corrected input data format: [Target Value, [field_apis], weight]
    # Using dummy target values for demonstration
    inputdata = [
        ['Global Media', ['Name'], 4],
        ['150 Chestnut Street', ['BillingStreet', 'BillingAddress.street', 'ShippingStreet'], 5],
        ['Toronto', ['BillingCity', 'BillingAddress.city', 'ShippingCity'], 1],
        ['L4B 1Y3', ['BillingPostalCode', 'BillingAddress.postalCode', 'ShippingPostalCode'], 1]
    ]
    
    # Mock records for testing
    mock_records = {
        "001": {"Id": "001", "Name": "Global Media Inc", "BillingCity": "Toronto"},
        "002": {"Id": "002", "Name": "Other Company", "BillingCity": "New York"}
    }
    
    print(run_similarity_analysis(mock_records, inputdata))