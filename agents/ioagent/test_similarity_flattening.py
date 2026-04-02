import unittest
from unittest.mock import MagicMock, AsyncMock
import sys
import os

# Add current directory to sys.path
sys.path.append(os.getcwd())

from Similarityanalysis import similarutyanalysistool

class TestSimilarityFlattening(unittest.TestCase):
    def setUp(self):
        self.tool = similarutyanalysistool()

    def test_flattening_logic(self):
        nested = {
            "a": 1,
            "b": {
                "c": 2,
                "d": {
                    "e": 3
                }
            }
        }
        flattened = self.tool._flatten_json(nested)
        expected = {
            "a": 1,
            "b.c": 2,
            "b.d.e": 3
        }
        self.assertEqual(flattened, expected)

    def test_preprocess_records(self):
        records = {
            "Rec1": {
                "Id": "Rec1",
                "Nested": {"Field": "Value"}
            },
            "Rec2": {
                "Id": "Rec2",
                "Flat": "Value"
            }
        }
        processed = self.tool.preprocess_records(records)
        self.assertEqual(processed["Rec1"]["Nested.Field"], "Value")
        self.assertEqual(processed["Rec2"]["Flat"], "Value")

    def test_run_with_data_nested(self):
        # Mock SOQL data with nested structure (AdQuoteLine style)
        mock_soql_data = {
            "Rec1": {
                "Id": "Rec1",
                "Name": "Line Item 1",
                "QuoteLineItem": {
                    "Quote": {
                        "Name": "Quote A"
                    },
                    "Product2": {
                        "Name": "Product X"
                    },
                    "TotalPrice": 100.0
                },
                "AdRequestedStartDate": "2023-01-01"
            },
            "Rec2": {
                "Id": "Rec2",
                "Name": "Line Item 2",
                "QuoteLineItem": {
                    "Quote": {
                        "Name": "Quote B"
                    },
                    "Product2": {
                        "Name": "Product Y"
                    },
                    "TotalPrice": 200.0
                }
            }
        }
        
        # Input data mapping
        # We want to map 'Line Item 1' to 'Name' and 'Quote A' to 'QuoteLineItem.Quote.Name'
        # Format: [Value, [Field_List], Weight]
        input_data = [
            ["Line Item 1", ["Name"], 5],
            ["Quote A", ["QuoteLineItem.Quote.Name"], 5],
            ["2023-01-01", ["AdRequestedStartDate"], 5]
        ]
        
        # Run the tool
        result = self.tool.run_with_data(mock_soql_data, input_data)
        
        print("\nTest Result for Rec1:", result.get("Rec1"))
        
        # Verify Rec1 is a match
        self.assertIn("Rec1", result)
        # Check score (index 1 of the value list)
        # result format: {Id: [[MatchDetails], Score]}
        score = result["Rec1"][1]
        self.assertGreater(score, 90, "Score should be high for exact match on nested fields")
        
        # Verify Rec2 is NOT a match (or low score) for this input
        if "Rec2" in result:
            score_rec2 = result["Rec2"][1]
            self.assertLess(score_rec2, score, "Rec2 should have lower score than Rec1")

if __name__ == "__main__":
    unittest.main()
