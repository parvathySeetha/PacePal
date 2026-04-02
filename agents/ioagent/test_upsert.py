from createRecords import create_records, upsert_record
import datetime

def test():
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    print("--- 1. Creating a Task ---")
    create_res = create_records("Task", {"Subject": f"Test Task {timestamp}", "Priority": "Normal"})
    print(f"Create Result: {create_res}")
    
    if create_res and 'id' in create_res:
        record_id = create_res['id']
        print(f"Created Record ID: {record_id}")
        
        print("\n--- 2. Upserting (Updating) the Task ---")
        update_res = upsert_record("Task", record_id, {"Description": f"Updated via upsert at {timestamp}", "Priority": "High"})
        print(f"Update Result: {update_res}")
        
    else:
        print("Failed to create record, skipping update test.")

    print("\n--- 3. Upserting (Creating) a new Task ---")
    create_upsert_res = upsert_record("Task", "", {"Subject": f"New Task via Upsert {timestamp}", "Priority": "Low"})
    print(f"Create (Upsert) Result: {create_upsert_res}")

if __name__ == "__main__":
    test()
