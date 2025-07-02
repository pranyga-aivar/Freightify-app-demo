import sys
import json
import os
import io
import traceback
from preprocessing_freightrates import FreightTableExtractor
from extraction import process_main_folder_structure_incremental


# Set UTF-8 encoding to handle Unicode characters (emojis, special chars)
os.environ['PYTHONIOENCODING'] = 'utf-8'
if hasattr(sys.stdout, 'buffer'):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
if hasattr(sys.stderr, 'buffer'):
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')


def main():
    try:
        # Read parameters from JSON file passed as command line argument
        params_file = sys.argv[1]
        
        with open(params_file, 'r', encoding='utf-8') as f:
            params = json.load(f)
        
        # Extract parameters
        file_path = params['file_path']
        ignored_sheets = params['ignored_sheets']
        custom_terms = params['custom_terms']
        file_stem = params['file_stem']
        
        # Write status file to indicate processing started
        status_file = f"{file_stem}_status.json"
        with open(status_file, 'w', encoding='utf-8') as f:
            json.dump({"status": "processing", "step": "preprocessing"}, f, ensure_ascii=False)

        
        # Preprocessing freightrates
        extractor = FreightTableExtractor(
            ignored_sheets=ignored_sheets,
            custom_terms=custom_terms if any(custom_terms.values()) else None
        )
        extractor.process_excel_file(file_path)
        
        # Update status
        with open(status_file, 'w', encoding='utf-8') as f:
            json.dump({"status": "processing", "step": "extraction"}, f, ensure_ascii=False)

        
        # Extraction
        main_folder = f"temp_inputfiles/{file_stem}_processed"
        output_main_folder = f"{file_stem}_processed_output"
        
        # Check for custom prompt file first, fallback to default
        custom_prompt_file = 'custom_prompt.txt'
        default_prompt_file = 'f9.txt'
        
        if os.path.exists(custom_prompt_file):
            extraction_prompt_path = custom_prompt_file
        else:
            extraction_prompt_path = default_prompt_file
        
        surge_charge_prompt_path = "s9.txt"
        context_filter_prompt_path = "context.txt"
        
        # Process the main folder structure with incremental writing
        process_main_folder_structure_incremental(
            main_folder_path=main_folder,
            extraction_prompt_path=extraction_prompt_path,
            surge_charge_prompt_path=surge_charge_prompt_path,
            context_filter_prompt_path=context_filter_prompt_path
        )
        
        # Write success status
        with open(status_file, 'w', encoding='utf-8') as f:
            json.dump({
                "status": "completed", 
                "output_folder": output_main_folder,
                "message": "Processing completed successfully!"
            }, f, ensure_ascii=False)
        
    except Exception as e:
        # Write error status
        error_msg = str(e)
        error_traceback = traceback.format_exc()
        
        status_file = f"{params.get('file_stem', 'unknown')}_status.json"
        with open(status_file, 'w', encoding='utf-8') as f:
            json.dump({
                "status": "error",
                "error": error_msg,
                "traceback": error_traceback
            }, f, ensure_ascii=False)

        
        # Exit with error code
        sys.exit(1)

if __name__ == "__main__":
    main()