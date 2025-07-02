import streamlit as st
import pandas as pd
import os
import io
import zipfile
import json
import subprocess
import sys
import time
import openpyxl
from extraction import process_main_folder_structure_incremental
from preprocessing_freightrates import FreightTableExtractor

st.title("Freightify - Excel processor")

def create_zip_from_folder(folder_path):
    """Create a ZIP file containing all files from folder and subfolders"""
    zip_buffer = io.BytesIO()
    
    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
        for root, dirs, files in os.walk(folder_path):
            for file in files:
                file_path = os.path.join(root, file)
                # Get relative path to maintain folder structure in ZIP
                arcname = os.path.relpath(file_path, folder_path)
                zip_file.write(file_path, arcname)
    
    zip_buffer.seek(0)
    return zip_buffer.getvalue()

def sheetname_checkbox(file_path):
    try:
        # Load workbook and get sheet names
        wb = openpyxl.load_workbook(file_path, data_only=True)
        sheet_names = wb.sheetnames
        
        st.success(f"File uploaded successfully! Found {len(sheet_names)} sheet(s)")
        
        # SIDEBAR: Sheet selection section
        st.sidebar.subheader("📋 Select Sheets that can be ignored while processing")
        
        st.session_state.ignored_sheets = []
        
        # Create checkboxes in sidebar
        for sheet_name in sheet_names:
            if st.sidebar.checkbox(sheet_name, key=f"sheet_{sheet_name}"):
                st.session_state.ignored_sheets.append(sheet_name)
        
        # Show results in main area
        if st.session_state.ignored_sheets:
            st.write("**Sheets to ignore:**", st.session_state.ignored_sheets)
            # if st.button("Process Selected Sheets"):
            #     st.success(f"Processing {len(selected_sheets)} sheet(s): {', '.join(selected_sheets)}")
            #     # Your processing logic here

        else:
            st.info("Please select sheets that can be ignored, if any, while processing from the sidebar")
            
    except Exception as e:
        st.error(f"Error reading Excel file: {str(e)}")

uploaded_file = st.file_uploader("Upload an Excel file", type=["xlsx", "xls"])
if uploaded_file is not None:
    # Create folder if it doesn't exist
    save_folder = "temp_inputfiles"
    os.makedirs(save_folder, exist_ok=True)
    
    # Save the file
    file_path = os.path.join(save_folder, uploaded_file.name)
    
    with open(file_path, "wb") as f:
        f.write(uploaded_file.getbuffer())
    
    st.success(f"File saved to: {file_path}")
    #choose sheets to process
    sheetname_checkbox(file_path)

# Button to show editable prompt
if "show_prompt" not in st.session_state:
    st.session_state.show_prompt = False

if st.button("View Prompt", type="secondary"):
    st.session_state.show_prompt = True

# Display and edit prompt if button clicked
default_prompt_file = 'f9.txt'
try:
    with open(default_prompt_file, 'r') as file:
        default_prompt = file.read()
except FileNotFoundError:
    default_prompt = "Default prompt file not found"

if st.session_state.show_prompt:
    prompt_text = st.text_area("Edit your prompt below:", value=default_prompt, height=400)
    
    if st.button("Save Custom Prompt"):
        custom_prompt_file = 'custom_prompt.txt'
        try:
            with open(custom_prompt_file, 'w') as file:
                file.write(prompt_text)
            st.success(f"Custom prompt saved")
        except Exception as e:
            st.error(f"Error saving prompt: {e}")

# # Sheets to ignore title
# st.sidebar.markdown("### 🚫 <span style='font-size:18px; font-weight:bold;'>Sheets to Ignore</span>", unsafe_allow_html=True)

# # Initialize ignored_sheets in session state if not exists
# if "ignored_sheets" not in st.session_state:
#     st.session_state.ignored_sheets = []

# # Input for adding a new sheet to ignore
# new_ignore_sheet = st.sidebar.text_input("Enter sheet name to ignore:")

# # Buttons to add and clear
# col1, col2 = st.sidebar.columns(2)
# with col1:
#     if st.button("Add Sheet", key="add_ignore"):
#         if new_ignore_sheet and new_ignore_sheet not in st.session_state.ignored_sheets:
#             st.session_state.ignored_sheets.append(new_ignore_sheet)
#             # st.rerun()

# with col2:
#     if st.button("Clear All", key="clear_ignore"):
#         st.session_state.ignored_sheets = []
#         # st.rerun()

# # Display current ignored sheets with remove buttons
# if st.session_state.ignored_sheets:
#     st.sidebar.markdown("#### Current Ignored Sheets:")
#     for i, sheet in enumerate(st.session_state.ignored_sheets):
#         col1, col2 = st.sidebar.columns([3, 1])
#         with col1:
#             st.markdown(f"<div style='font-size:16px'>{sheet}</div>", unsafe_allow_html=True)
#         with col2:
#             if st.button("❌", key=f"remove_{i}"):
#                 st.session_state.ignored_sheets.pop(i)
#                 # st.rerun()
# else:
#     st.sidebar.markdown("*No sheets to ignore*")

#custom column terms
st.sidebar.header("📝 Custom Column Terms (Optional)")
st.sidebar.write("Add custom terms to improve freight table detection accuracy:")

# Create expandable sections for each category in sidebar
with st.sidebar.expander("🌍 Location Terms"):
    st.write("Current terms: origin, destination, port, pol, pod, country, area, carrier, from, to, via, start")
    custom_location = st.text_input("Add location terms (comma-separated):", 
                                   placeholder="e.g., depot, terminal, hub",
                                   key="location_terms")

with st.sidebar.expander("📦 Container Terms"):
    st.write("Current terms: 20', 40', dc, hc, rf, rq, box, soc, dry, reefer, container")
    custom_container = st.text_input("Add container terms (comma-separated):", 
                                    placeholder="e.g., teu, feu, lcl",
                                    key="container_terms")

with st.sidebar.expander("💰 Rate Terms"):
    st.write("Current terms: rate, currency, charges, price, cost, amount, fee, tariff")
    custom_rate = st.text_input("Add rate terms (comma-separated):", 
                               placeholder="e.g., freight, fare, levy",
                               key="rate_terms")

with st.sidebar.expander("🚚 Logistics Terms"):
    st.write("Current terms: mode, term, code, routing, service, transit")
    custom_logistics = st.text_input("Add logistics terms (comma-separated):", 
                                    placeholder="e.g., incoterm, delivery, shipment",
                                    key="logistics_terms")

# Process custom terms
def parse_custom_terms(text):
    """Parse comma-separated terms and clean them"""
    if not text.strip():
        return []
    return [term.strip().lower() for term in text.split(',') if term.strip()]

custom_terms = {
    'location': parse_custom_terms(custom_location),
    'container': parse_custom_terms(custom_container),
    'rate': parse_custom_terms(custom_rate),
    'logistics': parse_custom_terms(custom_logistics)
}

# Show preview of custom terms if any are added - in sidebar
if any(custom_terms.values()):
    st.sidebar.subheader("🔍 Custom Terms Preview")
    for category, terms in custom_terms.items():
        if terms:
            st.sidebar.write(f"**{category.title()}:** {', '.join(terms)}")


if uploaded_file is not None:
    st.markdown("---")
    st.markdown("## Process File")
    
    # Display file info
    st.info(f"📁 **File uploaded:** {uploaded_file.name} ({uploaded_file.size} bytes)")
    #custom terms display
    # Add this right before the "Process Excel File" button
    if any(custom_terms.values()):
        st.info("🎯 Custom column terms will be used to enhance freight table detection")
        with st.expander("View terms that will be used"):
            for category, terms in custom_terms.items():
                if terms:
                    st.write(f"**{category.title()}:** {', '.join(terms)}")

    # Initialize session state
    if "processing_complete" not in st.session_state:
        st.session_state.processing_complete = False
    if "file_stem" not in st.session_state:
        st.session_state.file_stem = ""
    file_stem = os.path.splitext(uploaded_file.name)[0]
    st.write(file_stem)
    output_main_folder = f"{file_stem}_processed_output"

    # Initialize session state at the top of your file
    if "show_download" not in st.session_state:
        st.session_state.show_download = False

    # Process button
    # Initialize processing state
    if "is_processing" not in st.session_state:
        st.session_state.is_processing = False
    if "process_started" not in st.session_state:
        st.session_state.process_started = False

    # Process button
    if st.button("🔄 Process Excel File", type="primary", use_container_width=True, disabled=st.session_state.is_processing):
        try:
            # Prepare parameters for background process
            file_stem = os.path.splitext(uploaded_file.name)[0]
            st.session_state.file_stem = file_stem
            
            params = {
                'file_path': file_path,
                'ignored_sheets': st.session_state.ignored_sheets,
                'custom_terms': custom_terms,
                'file_stem': file_stem
            }
            
            # Save parameters to JSON file
            params_file = f"{file_stem}_params.json"
            with open(params_file, 'w') as f:
                json.dump(params, f)
            
            # Start background process
            process = subprocess.Popen([
                sys.executable, "background_processor.py", params_file
            ], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            
            st.session_state.is_processing = True
            st.session_state.process_started = True
            st.success("🚀 Processing started in background! Use the refresh button below to check status.")
            
        except FileNotFoundError:
            st.error("❌ Error: background_processor.py not found. Please ensure the file exists.")
        except Exception as e:
            st.error(f"❌ Error starting background process: {str(e)}")

    # Status checking and refresh button
    if st.session_state.process_started:
        col1, col2 = st.columns([1, 3])
        
        with col1:
            if st.button("🔄 Refresh Status", type="secondary"):
                st.rerun()
        
        # Check processing status
        if st.session_state.is_processing:
            status_file = f"{st.session_state.file_stem}_status.json"
            
            if os.path.exists(status_file):
                try:
                    with open(status_file, 'r') as f:
                        status_data = json.load(f)
                    
                    if status_data['status'] == 'processing':
                        step = status_data.get('step', 'unknown')
                        if step == 'preprocessing':
                            st.info("⏳ Step 1/2: Preprocessing freight rates...")
                        elif step == 'extraction':
                            st.info("⏳ Step 2/2: Extracting data...")
                        else:
                            st.info("⏳ Processing...")
                            
                    elif status_data['status'] == 'completed':
                        st.success("✅ Processing completed successfully!")
                        st.session_state.is_processing = False
                        st.session_state.show_download = True
                        
                        # Clean up temporary files
                        try:
                            os.remove(status_file)
                            os.remove(f"{st.session_state.file_stem}_params.json")
                        except:
                            pass
                            
                    elif status_data['status'] == 'error':
                        st.error(f"❌ Processing failed: {status_data.get('error', 'Unknown error')}")
                        st.session_state.is_processing = False
                        
                        # Show detailed error in expander
                        if 'traceback' in status_data:
                            with st.expander("View detailed error"):
                                st.code(status_data['traceback'])
                        
                        # Clean up temporary files
                        try:
                            os.remove(status_file)
                            os.remove(f"{st.session_state.file_stem}_params.json")
                        except:
                            pass
                            
                except json.JSONDecodeError:
                    st.warning("⚠️ Status file corrupted. Please try again.")
                except Exception as e:
                    st.error(f"❌ Error reading status: {str(e)}")
            else:
                st.info("⏳ Starting background process...")

    # Simple download button
    if st.session_state.show_download and not st.session_state.is_processing:
        #zip file
        file_stem = st.session_state.file_stem if hasattr(st.session_state, 'file_stem') else os.path.splitext(uploaded_file.name)[0]
        output_main_folder = f"{file_stem}_processed_output"
        
        root_folder = output_main_folder

        # # Initialize session state for selected file display
        # if "selected_json_file" not in st.session_state:
        #     st.session_state.selected_json_file = None

        # # Collect all JSON files first
        # json_files = []
        # for dirpath, dirnames, filenames in os.walk(root_folder):
        #     for file in filenames:
        #         if file.endswith(".json"):
        #             file_path = os.path.join(dirpath, file)
        #             # Store both the file name and full path
        #             relative_path = os.path.relpath(dirpath, root_folder)
        #             display_name = f"{relative_path}/{file}" if relative_path != "." else file
        #             json_files.append({
        #                 "display_name": display_name,
        #                 "file_path": file_path,
        #                 "file_name": file
        #             })

        # if json_files:
        #     st.subheader("📁 Processed Files")
        #     st.write(f"Found {len(json_files)} JSON file(s). Click on a file to view its contents:")
            
        #     # Create buttons for each JSON file
        #     cols = st.columns(min(3, len(json_files)))  # Max 3 columns
            
        #     for idx, file_info in enumerate(json_files):
        #         col_idx = idx % 3
        #         with cols[col_idx]:
        #             # Create a unique button for each file
        #             if st.button(
        #                 f"📄 {file_info['file_name']}", 
        #                 key=f"json_btn_{idx}",
        #                 help=f"View contents of {file_info['display_name']}"
        #             ):
        #                 st.session_state.selected_json_file = file_info
            
        #     # Display selected file content
        #     if st.session_state.selected_json_file:
        #         selected_file = st.session_state.selected_json_file
                
        #         st.markdown("---")
        #         st.subheader(f"📄 {selected_file['display_name']}")
                
        #         try:
        #             with open(selected_file['file_path'], "r", encoding="utf-8") as f:
        #                 data = json.load(f)

        #             # Add a clear button
        #             col1, col2 = st.columns([1, 4])
        #             with col1:
        #                 if st.button("❌ Clear View", key="clear_json_view"):
        #                     st.session_state.selected_json_file = None
        #                     st.rerun()
                    
        #             # Normalize and display JSON data
        #             if isinstance(data, dict):
        #                 df = pd.json_normalize(data)
        #             elif isinstance(data, list):
        #                 df = pd.json_normalize(data)
        #             else:
        #                 df = pd.DataFrame([data])

        #             # Display as both table and raw JSON
        #             tab1, tab2 = st.tabs(["📊 Table View", "📋 Raw JSON"])
                    
        #             with tab1:
        #                 st.dataframe(df, use_container_width=True)
                        
        #                 # Add download button for this specific file
        #                 csv_data = df.to_csv(index=False)
        #                 st.download_button(
        #                     label=f"📥 Download {selected_file['file_name']} as CSV",
        #                     data=csv_data,
        #                     file_name=f"{os.path.splitext(selected_file['file_name'])[0]}.csv",
        #                     mime="text/csv"
        #                 )
                    
        #             with tab2:
        #                 st.json(data)

        #         except Exception as e:
        #             st.error(f"❌ Error reading {selected_file['file_path']}: {e}")
        # else:
        #     st.warning("⚠️ No JSON files found in the output folder.")


        # Initialize session state for selected file display
        if "selected_json_file" not in st.session_state:
            st.session_state.selected_json_file = None

        # Organize JSON files by folder
        json_files_by_folder = {}
        for dirpath, dirnames, filenames in os.walk(root_folder):
            json_files_in_folder = []
            for file in filenames:
                if file.endswith(".json"):
                    file_path = os.path.join(dirpath, file)
                    json_files_in_folder.append({
                        "file_name": file,
                        "file_path": file_path
                    })
            
            if json_files_in_folder:
                relative_path = os.path.relpath(dirpath, root_folder)
                folder_name = "Root" if relative_path == "." else relative_path
                json_files_by_folder[folder_name] = json_files_in_folder

        if json_files_by_folder:
            st.subheader("📁 Processed Files")
            
            # Display files organized by folders
            for folder_name, files in json_files_by_folder.items():
                with st.expander(f"📂 {folder_name} ({len(files)} files)", expanded=True):
                    cols = st.columns(min(3, len(files)))
                    
                    for idx, file_info in enumerate(files):
                        col_idx = idx % 3
                        with cols[col_idx]:
                            button_key = f"json_btn_{folder_name}_{idx}"
                            if st.button(
                                f"📄 {file_info['file_name']}", 
                                key=button_key,
                                help=f"View contents of {file_info['file_name']}"
                            ):
                                st.session_state.selected_json_file = {
                                    **file_info,
                                    "folder": folder_name
                                }
            
            # Display selected file content (same as above)
            if st.session_state.selected_json_file:
                # ... (same display logic as previous example)
                selected_file = st.session_state.selected_json_file
                
                st.markdown("---")
                # st.subheader(f"📄 {selected_file['display_name']}")
                display_name = f"{selected_file['folder']}/{selected_file['file_name']}" if selected_file['folder'] != "Root" else selected_file['file_name']
                st.subheader(f"📄 {display_name}")
                
                try:
                    with open(selected_file['file_path'], "r", encoding="utf-8") as f:
                        data = json.load(f)

                    # Add a clear button
                    col1, col2 = st.columns([1, 4])
                    with col1:
                        if st.button("❌ Clear View", key="clear_json_view"):
                            st.session_state.selected_json_file = None
                            st.rerun()
                    
                    # Normalize and display JSON data
                    if isinstance(data, dict):
                        df = pd.json_normalize(data)
                    elif isinstance(data, list):
                        df = pd.json_normalize(data)
                    else:
                        df = pd.DataFrame([data])

                    # Display as both table and raw JSON
                    tab1, tab2 = st.tabs(["📊 Table View", "📋 Raw JSON"])
                    
                    with tab1:
                        st.dataframe(df, use_container_width=True)
                        
                        # # Add download button for this specific file
                        # csv_data = df.to_csv(index=False)
                        # st.download_button(
                        #     label=f"📥 Download {selected_file['file_name']} as CSV",
                        #     data=csv_data,
                        #     file_name=f"{os.path.splitext(selected_file['file_name'])[0]}.csv",
                        #     mime="text/csv"
                        # )
                    
                    with tab2:
                        st.json(data)

                except Exception as e:
                    st.error(f"❌ Error reading {selected_file['file_path']}: {e}")
        else:
            st.warning("⚠️ No JSON files found in the output folder.")







        # # Walk through all subfolders and get JSON files
        # for dirpath, dirnames, filenames in os.walk(root_folder):
        #     for file in filenames:
        #         if file.endswith(".json"):
        #             file_path = os.path.join(dirpath, file)
        #             try:
        #                 with open(file_path, "r", encoding="utf-8") as f:
        #                     data = json.load(f)

        #                 # Normalize if nested JSON
        #                 if isinstance(data, dict):
        #                     df = pd.json_normalize(data)
        #                 elif isinstance(data, list):
        #                     df = pd.json_normalize(data)
        #                 else:
        #                     df = pd.DataFrame([data])

        #                 st.subheader(f"📄 {file}")
        #                 st.dataframe(df)

        #             except Exception as e:
        #                 st.error(f"❌ Error reading {file_path}: {e}")





        # if os.path.exists(output_main_folder):
        #     # Count files
        #     file_count = sum([len(files) for r, d, files in os.walk(output_main_folder)])
            
        #     if file_count > 0:
        #         st.write(f"Found {file_count} files in folder and subfolders")
                
        #         # Create and download ZIP file
        #         try:
        #             zip_data = create_zip_from_folder(output_main_folder)
                    
        #             st.download_button(
        #                 label=f"📦 Download All Files as ZIP ({file_count} files)",
        #                 data=zip_data,
        #                 file_name=f"{os.path.basename(output_main_folder)}_all_files.zip",
        #                 mime="application/zip"
        #             )
        #         except Exception as e:
        #             st.error(f"❌ Error creating ZIP file: {str(e)}")
        #     else:
        #         st.error("No files found in the folder")
        # else:
        #     st.error("Output folder not found")
    # if st.session_state.show_download and not st.session_state.is_processing:

    #     #     st.stop()
    #     # Ensure file_stem is initialized correctly before accessing
    #     # if "file_stem" not in st.session_state or not st.session_state.file_stem:
    #     #     if uploaded_file is not None:
    #     #         st.session_state.file_stem = os.path.splitext(uploaded_file.name)[0]
    #     #     else:
    #     #         st.error("❌ No file name found. Please re-upload the file to continue.")
    #     #         st.stop()

    #     # file_stem = st.session_state.file_stem if hasattr(st.session_state, 'file_stem') else os.path.splitext(uploaded_file.name)[0]
    #     if "file_stem" in st.session_state and st.session_state.file_stem:
    #          file_stem = st.session_state.file_stem
    #     elif uploaded_file is not None:
    #         file_stem = os.path.splitext(uploaded_file.name)[0]
    #     else:
    #         st.error("❌ No file name found. Please re-upload the file to continue.")
    #         st.stop()
    #     output_main_folder = f"{file_stem}_processed_output"
    #     root_folder = output_main_folder

    #     # Walk through all subfolders and get JSON files
    #     for dirpath, dirnames, filenames in os.walk(root_folder):
    #         for file in filenames:
    #             if file.endswith(".json"):
    #                 file_path = os.path.join(dirpath, file)
    #                 try:
    #                     with open(file_path, "r", encoding="utf-8") as f:
    #                         data = json.load(f)

    #                     # Normalize if nested JSON
    #                     if isinstance(data, dict):
    #                         df = pd.json_normalize(data)
    #                     elif isinstance(data, list):
    #                         df = pd.json_normalize(data)
    #                     else:
    #                         df = pd.DataFrame([data])

    #                     st.subheader(f"📄 {file}")
    #                     st.dataframe(df)

    #                 except Exception as e:
    #                     st.error(f"❌ Error reading {file_path}: {e}")


else:
    st.info("👆 Please upload an Excel file to begin processing")
