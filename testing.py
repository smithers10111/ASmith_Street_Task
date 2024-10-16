""""
This testing script is irrelevant to the task it was purely for my own testing purposes.

"""

import pandas as pd

# # Define the headers
# headers = [
#     "Transaction_unique_identifier",
#     "Price",
#     "Date_of_Transfer",
#     "Postcode",
#     "Property_Type",
#     "Old_New",
#     "Duration",
#     "PAON",
#     "SAON",
#     "Street",
#     "Locality",
#     "Town_City",
#     "District",
#     "County",
#     "PPD_Category_Type",
#     "Record_Status"
# ]

# # Read the CSV file with the specified headers
# df = pd.read_csv('pp-complete.csv', names=headers)

# # Group by address and postcode, and count the number of transactions
# address_counts = df.groupby(['PAON', 'SAON', 'Postcode']).size().reset_index(name='count')

# # Filter the addresses with more than one transaction
# filtered_addresses = address_counts[address_counts['count'] > 1]

# # Merge the filtered addresses with the original dataframe
# filtered_rows = df.merge(filtered_addresses, on=['PAON', 'SAON', 'Postcode'])

# # Write the filtered rows to a new CSV file
# filtered_rows.to_csv('filtered_rows.csv', index=False)

# # Read the filtered_rows.csv file
# df = pd.read_csv('filtered_rows.csv')

# # Filter the dataframe to only include rows where Postcode starts with N12
# df_filtered = df[df['Postcode'].str.startswith('N12')]

# # Save the filtered dataframe to a new csv file
# df_filtered.to_csv('filtered_rows_N12.csv', index=False)




"""" Left this in to show testing for whether Street/District/County/Town_City 
     is consistent with a certain combination of Postcode/PAON/SAON
     
     If we change the serached values, the results will be different
"""


import json

file_path = 'output/output-00000-of-00001.ndjson'

# Initialize an empty list to store the parsed JSON objects
data = []


# Step 1: Read the multi-line JSON file and concatenate lines into full JSON objects
with open(file_path, 'r', encoding='utf-8') as file:
    json_lines = []
    current_object = ''
    open_braces = 0


    for line in file:
        # Count the opening and closing braces
        open_braces += line.count('{')
        open_braces -= line.count('}')
        
        # Concatenate lines into a single object
        current_object += line.strip()


        # Once all braces are balanced, we've reached the end of a JSON object
        if open_braces == 0 and current_object:
            try:
                # Parse the complete JSON object
                json_object = json.loads(current_object)
                data.append(json_object)  # Add to the data list
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON: {e}")
                print(f"Offending object: {current_object}")
            # Reset the current object to start a new one
            current_object = ''


# Step 2: Find addresses and check for inconsistencies
addresses = {}
for obj in data:
    # Create the address tuple (postcode, PAON, SAON)
    address = (obj['Postcode'], obj.get('PAON'), obj.get('SAON'))  # Use .get() to handle missing keys safely
    if address not in addresses:
        addresses[address] = set()
    for transaction in obj['transactions']:
        addresses[address].add(transaction['County'])  # Collect the streets for each address


# Step 3: Find addresses with inconsistent street names
inconsistent_addresses = {address: streets for address, streets in addresses.items() if len(streets) > 1}


# Step 4: Output the results
for address, streets in inconsistent_addresses.items():
    print(f"Inconsistent streets for address {address}: {streets}")