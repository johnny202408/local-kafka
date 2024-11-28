import json
import random

# Sample data template
sample_data = [
    {
        "item_id": "12345",
        "category_id": "67890",
        "name": "Widget A",
        "dept": "Electronics"
    },
    {
        "item_id": "23456",
        "category_id": "78901",
        "name": "Gadget B",
        "dept": "Home & Kitchen"
    },
    {
        "item_id": "34567",
        "category_id": "89012",
        "name": "Tool C",
        "dept": "Hardware"
    },
    {
        "item_id": "45678",
        "category_id": "90123",
        "name": "Appliance D",
        "dept": "Appliances"
    }
]

# Function to generate random data
def generate_random_data():
    return [
        {
            "item_id": str(random.randint(10000, 99999)),
            "category_id": str(random.randint(10000, 99999)),
            "name": f"Item {chr(random.randint(65, 90))}",
            "dept": random.choice(["Electronics", "Home & Kitchen", "Hardware", "Appliances"])
        } for _ in range(4)
    ]

# Generate 10 JSON files
for i in range(10):
    data = generate_random_data()
    with open(f"sample_data/sample_data_0{i+1}.json", "w") as json_file:
        json.dump(data, json_file, indent=2)

print("10 JSON files generated successfully.")
