import json

# Read the input data from input.json
with open('coins_list.json', 'r') as input_file:
    data = json.load(input_file)

# Convert to desired format
result = {item['symbol']: item['id'] for item in data}

# Write the result to output.json
with open('symbol_id_map.json', 'w') as output_file:
    json.dump(result, output_file, indent=4)

print("Data has been converted and written to output.json")
