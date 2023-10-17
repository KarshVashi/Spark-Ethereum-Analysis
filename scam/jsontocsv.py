import json
import csv

# Open the JSON file for reading
with open('scams.json', 'r') as f:
    # Load the JSON data into a variable
    data = json.load(f)

# Open a new CSV file for writing
with open('scams2.csv', 'w', newline='') as f:
    # Create a CSV writer object
    writer = csv.writer(f)

    # Write the header row
    # writer.writerow(['id', 'name', 'url', 'coin', 'category', 'subcategory', 'addresses', 'reporter', 'status'])
    # print(data['result'].keys())
    
    # Loop through the JSON data
    for item in data['result'].values():
        
        # print(item['addresses'])
        # print(item)
        # print(item['id'])
        if item['category'] != 'Scam':
            category = item['category']
        else:
            category = 'Scamming'
        if 'subcategory' in item:
            subcategory = str(item['subcategory'])
        else:
            subcategory = ""
        if 'reporter' in item:
            reporter = str(item['reporter'])
        else:
            subcategory = ""
        for address in item['addresses']:
            writer.writerow([item['id'], item['name'], item['url'], item['coin'], category, subcategory, address, reporter,  item['status']])
