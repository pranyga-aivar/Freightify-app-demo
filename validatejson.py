import json

# Load JSON from file
with open(r'C:\Users\HP\AppData\Local\Temp\de26cd9e-f965-42cd-add6-5749012d0b0e_IET - ONE Germany Rate Sheet 01.04.2025 - 30.04.2025---2833-2025-04-08T06_40_32.200Z_processed_output_all_files.zip.b0e\IET - SB\freight_rates.json') as f:
    data = json.load(f)

# Count key-value pairs at the top level
total_pairs = len(data)
print(f"Total key-value pairs: {total_pairs}")
