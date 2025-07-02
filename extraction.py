from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
import boto3
import json
import os
import re
import threading

load_dotenv()

AWS_REGION = os.getenv("AWS_REGION")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

bedrock_client = boto3.client(
    "bedrock-runtime",
    region_name=AWS_REGION,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)

def extract_json_from_backticks(text: str) -> dict:
    pattern = r"``````"
    match = re.search(pattern, text, re.DOTALL)
    if not match:
        raise ValueError("No JSON block found in the provided text.")
    return json.loads(match.group(1))


def call_nova_pro_converse_cached(
    static_prompt: str,
    user_input: str,
    *,
    model_id: str = "amazon.nova-pro-v1:0",
    temperature: float = 0.7,
    max_tokens: int = 4096,
    top_p: float = 0.9,
    stop_sequences: list[str] | None = None,
) -> str:
    """
    Single-turn Converse call to Amazon Nova Pro with Bedrock prompt-caching.

    The `static_prompt` (first segment) is cached on the first request,
    so later calls that reuse the same string are cheaper and faster.
    """

    # 1️⃣ Messages — each content object has ONE union key only
    messages = [
        {
            "role": "user",
            "content": [
                {"text": static_prompt},                # ← cached part
                {"cachePoint": {"type": "default"}},    # ← cache marker
                {"text": user_input},                   # ← fresh input
            ],
        }
    ]

    # 2️⃣ Generation controls (only allowed keys)
    inference_config = {
        "maxTokens": max_tokens,
        "temperature": temperature,
        "topP": top_p,
    }
    if stop_sequences:
        inference_config["stopSequences"] = stop_sequences

    # 3️⃣ Converse call
    response = bedrock_client.converse(
        modelId=model_id,
        messages=messages,
        inferenceConfig=inference_config,
    )

    # 4️⃣ Extract assistant text (Nova always returns a list in content)
    assistant_segments = response["output"]["message"]["content"]
    assistant_text = "".join(seg.get("text", "") for seg in assistant_segments)
    usage = response['usage']
    # print(usage)
    return assistant_text,usage


def call_bedrock_claude(static_prompt, user_input, model_id="anthropic.claude-3-7-sonnet-20250219-v1:0", temperature=0.5, max_tokens=4096):
    """
    Uses Claude 3.7 Sonnet with Bedrock and prompt caching.
    """
    """
    Uses Claude 3.7 Sonnet with Bedrock and prompt caching.
    """
    request_body = {
        "anthropic_version": "bedrock-2023-05-31",
        "messages": [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": static_prompt},
                    {"cachePoint": {"type": "default"}},  # ✅ correct structure
                    {"type": "text", "text": user_input}
                ]
            }
        ],
        "max_tokens": max_tokens,
        "temperature": temperature
    }

    response = bedrock_client.invoke_model(
        modelId=model_id,
        contentType="application/json",
        accept="application/json",
        body=json.dumps(request_body)
    )

    response_body = json.loads(response["body"].read())
    return response_body["content"][0]["text"]



def find_freight_and_context_files(subfolder_path):
    """Find freight_table and context files in a subfolder"""
    files = [f for f in os.listdir(subfolder_path) if f.endswith(('.xlsx', '.xls'))]
    
    freight_file = None
    context_file = None
    
    for file in files:
        file_lower = file.lower()
        if "freight_table" in file_lower:
            freight_file = os.path.join(subfolder_path, file)
        elif "context" in file_lower:
            context_file = os.path.join(subfolder_path, file)
    
    return freight_file, context_file

def write_json_record_to_file(file_handle, record, is_first, file_lock):
    """Thread-safe function to write JSON record to file"""
    with file_lock:
        if not is_first[0]:
            file_handle.write(",\n")
        file_handle.write(json.dumps(record, ensure_ascii=False, indent=2))
        file_handle.flush()  # Ensure immediate write to disk
        is_first[0] = False

def process_subfolder_pair_incremental(subfolder_path, subfolder_name, extraction_prompt_path, surge_charge_prompt_path, output_base_folder,context_filter_prompt_path=None):
    """Process a single subfolder with incremental JSON writing"""
    print(f"\n📁 Processing subfolder: {subfolder_name}")
    
    # Find freight and context files
    freight_file, context_file = find_freight_and_context_files(subfolder_path)
    
    if not freight_file or not context_file:
        print(f"❌ Required files not found in {subfolder_name}")
        return False
    
    print(f"📊 Freight file: {os.path.basename(freight_file)}")
    print(f"📋 Context file: {os.path.basename(context_file)}")
    
    # Load prompt templates
    # with open(extraction_prompt_path, "r") as f:
    #     extraction_prompt_template = f.read().strip()

    with open(extraction_prompt_path, "r", encoding="utf-8") as f:
        extraction_prompt_template = f.read().strip()

    # Create output subfolder
    output_subfolder = os.path.join(output_base_folder, subfolder_name)
    os.makedirs(output_subfolder, exist_ok=True)
    
    try:
        # Read freight rate file
        df_freight = pd.read_excel(freight_file, dtype=str).fillna("")
        print(f"✅ Loaded freight data: {len(df_freight)} rows")
        
        # Read context file
        df_context = pd.read_excel(context_file, dtype=str).fillna("")
        context_csv = df_context.to_csv(index=False) if not df_context.empty else ""
        print(f"✅ Loaded context data: {len(df_context)} rows")
        
        if len(df_freight) < 1:
            print(f"❌ Insufficient data in freight file for {subfolder_name}")
            return False
        
        # Get header reference
        header_reference_csv = df_freight.head(2).to_csv(index=False)

        #load context filter prompt
        context_filter_prompt = None
        # if context_filter_prompt_path and os.path.exists(context_filter_prompt_path):
        #     with open(context_filter_prompt_path, "r") as f:
        #         context_filter_prompt = f.read().strip()
        
        if context_filter_prompt_path and os.path.exists(context_filter_prompt_path):
            with open(context_filter_prompt_path, "r", encoding="utf-8") as f:
                context_filter_prompt = f.read().strip()

        #extract relevant context only
        # Filter context using LLM if filter prompt is provided
        filtered_context_csv = context_csv
        if context_filter_prompt and context_csv:
            print(f"🔍 Filtering context data using LLM...")
            try:
                filtered_context, usage = call_nova_pro_converse_cached(context_filter_prompt, context_csv)
                print("Context Filter - Cache hit?", usage.get("promptCacheHit"))
                print("Context Filter - Input tokens:", usage.get("inputTokens"))
        
                # Try to parse as JSON first, then fallback to plain text
                try:
                    parsed_context = json.loads(filtered_context)
                    if isinstance(parsed_context, dict) and 'filtered_context' in parsed_context:
                        filtered_context_csv = parsed_context['filtered_context']
                    elif isinstance(parsed_context, list):
                        # Convert list back to CSV format
                        filtered_df = pd.DataFrame(parsed_context)
                        filtered_context_csv = filtered_df.to_csv(index=False)
                    else:
                        filtered_context_csv = str(parsed_context)
                except json.JSONDecodeError:
                    # Use the raw response if not valid JSON
                    filtered_context_csv = filtered_context
        
                print(f"✅ Context filtered successfully")
                print(f"📏 Original context length: {len(context_csv)} chars")
                print(f"📏 Filtered context length: {len(filtered_context_csv)} chars")
        
            except Exception as e:
                print(f"⚠️ Error filtering context, using original: {e}")
                filtered_context_csv = context_csv


        extraction_prompt = extraction_prompt_template.replace("{{METADATA_CONTEXT_HERE}}", filtered_context_csv)
        extraction_prompt = extraction_prompt.replace("{{HEADER_REFRENCE}}", header_reference_csv)
        
        # Output file path
        freight_rates_output_path = os.path.join(output_subfolder, "freight_rates.json")
        
        print(f"🔄 Processing {len(df_freight)} rows for {subfolder_name}...")
        print(f"📝 Writing results incrementally to: {freight_rates_output_path}")
        
        # Open JSON file for incremental writing
        with open(freight_rates_output_path, "w", encoding="utf-8") as json_file:
            json_file.write("[\n")  # Start JSON array
            json_file.flush()
            
            is_first = [True]  # Use list to make it mutable for nested function
            file_lock = threading.Lock()  # Thread-safe file writing
            
            # Process with ThreadPoolExecutor
            with ThreadPoolExecutor(max_workers=5) as executor:
                # Submit all tasks
                future_to_row = {}
                
                for idx, row in df_freight.iterrows():
                    # Convert current row to CSV
                    row_csv = row.to_frame().T.to_csv(index=False, header=False)
                    
                    # Prepare extraction prompt
                    # extraction_prompt = extraction_prompt_template.replace("{{TABULAR_DATA_CHUNK_HERE}}", row_csv)
                    
                    future = executor.submit(call_nova_pro_converse_cached, extraction_prompt, row_csv)
                    future_to_row[future] = idx
                
                # Process results as they complete
                for future in as_completed(future_to_row):
                    idx = future_to_row[future]
                    try:
                        result,usage = future.result()
                        print("Cache hit?       ", usage.get("promptCacheHit"))
                        print("Cached tokens    ", usage.get("cachedTokens"))
                        print("Input tokens     ", usage.get("inputTokens"))

                        # Parse JSON response
                        try:
                            records = json.loads(result)
                        except json.JSONDecodeError:
                            try:
                                records = extract_json_from_backticks(result)
                            except:
                                records = [{"raw_response": result, "row_index": idx}]
                        
                        # Write each record immediately
                        if isinstance(records, list):
                            for record in records:
                                write_json_record_to_file(json_file, record, is_first, file_lock)
                            print(f"✅ {subfolder_name} - Row {idx} → Wrote {len(records)} JSON object(s) to file")
                        else:
                            write_json_record_to_file(json_file, records, is_first, file_lock)
                            print(f"✅ {subfolder_name} - Row {idx} → Wrote 1 JSON object to file")
                            
                    except Exception as e:
                        print(f"❌ Error processing row {idx} in {subfolder_name}: {e}")
                        error_record = {"error": str(e), "row_index": idx, "subfolder": subfolder_name}
                        write_json_record_to_file(json_file, error_record, is_first, file_lock)
            
            # Close JSON array
            json_file.write("\n]")
            json_file.flush()
        
        print(f"✅ Successfully completed {subfolder_name}")
        print(f"💾 Final JSON file saved: {freight_rates_output_path}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error processing {subfolder_name}: {e}")
        return False

def process_main_folder_structure_incremental(main_folder_path, extraction_prompt_path, surge_charge_prompt_path, context_filter_prompt_path=None):
    """Process main folder with incremental JSON writing"""
    
    if not os.path.exists(main_folder_path):
        print(f"❌ Main folder does not exist: {main_folder_path}")
        return
    
    # Create output folder name
    main_folder_name = os.path.basename(main_folder_path.rstrip('/\\'))
    output_main_folder = f"{main_folder_name}_output"
    os.makedirs(output_main_folder, exist_ok=True)
    
    # Get all subfolders
    subfolders = []
    for item in os.listdir(main_folder_path):
        item_path = os.path.join(main_folder_path, item)
        if os.path.isdir(item_path):
            subfolders.append((item_path, item))
    
    if not subfolders:
        print(f"⚠️ No subfolders found in {main_folder_path}")
        return
    
    print(f"🎯 Found {len(subfolders)} subfolders to process")
    print(f"📁 Input folder: {main_folder_path}")
    print(f"📁 Output folder: {output_main_folder}")
    print("📝 JSON files will be written incrementally as results are received")
    
    successful_subfolders = 0
    failed_subfolders = 0
    
    for subfolder_path, subfolder_name in subfolders:
        success = process_subfolder_pair_incremental(
            subfolder_path=subfolder_path,
            subfolder_name=subfolder_name,
            extraction_prompt_path=extraction_prompt_path,
            surge_charge_prompt_path=surge_charge_prompt_path,
            output_base_folder=output_main_folder,
            context_filter_prompt_path=context_filter_prompt_path
        )
        
        if success:
            successful_subfolders += 1
        else:
            failed_subfolders += 1
    
    # print(f"\n🎯 Final Processing Summary:")
    # print(f"   ✅ Successfully processed: {successful_subfolders} subfolders")
    # print(f"   ❌ Failed to process: {failed_subfolders} subfolders")
    # print(f"   📁 Output folder: {output_main_folder}")
    
    # # Create summary file
    # summary = {
    #     "input_folder": main_folder_path,
    #     "output_folder": output_main_folder,
    #     "total_subfolders": len(subfolders),
    #     "successful_subfolders": successful_subfolders,
    #     "failed_subfolders": failed_subfolders,
    #     "processed_subfolders": [name for _, name in subfolders],
    #     "processing_mode": "incremental_writing"
    # }
    
    # summary_path = os.path.join(output_main_folder, "processing_summary.json")
    # with open(summary_path, "w", encoding="utf-8") as f:
    #     json.dump(summary, f, ensure_ascii=False, indent=2)
    
    # print(f"📋 Processing summary saved to: {summary_path}")



if __name__ == "__main__":
    # Configuration
    main_folder = "all_templates/Cosco Shipping Lines Germany - FAK Ratesheet REEFER - 01.04. - 30.04.2025---3314-2025-04-08T08:28:47.958Z_processed"  # Main folder containing subfolders
    # extraction_prompt_path = "f9.txt"
    # Check for custom prompt file first, fallback to default
    custom_prompt_file = 'custom_prompt.txt'
    default_prompt_file = 'f9.txt'

    if os.path.exists(custom_prompt_file):
        extraction_prompt_path = custom_prompt_file
        print(f"Using custom prompt: {custom_prompt_file}")  # Optional: for debugging
    else:
        extraction_prompt_path = default_prompt_file
        print(f"Using default prompt: {default_prompt_file}")  # Optional: for debugging

    surge_charge_prompt_path = "s9.txt"
    context_filter_prompt_path="context.txt"
    
    # Process the main folder structure with incremental writing
    process_main_folder_structure_incremental(
        main_folder_path=main_folder,
        extraction_prompt_path=extraction_prompt_path,
        surge_charge_prompt_path=surge_charge_prompt_path,
        context_filter_prompt_path=context_filter_prompt_path
    )
