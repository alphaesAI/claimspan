import os
import sys
import shutil
from pathlib import Path

# Setup workspace environment paths - Use absolute path for Repos
root_dir = Path("/Workspace/Repos/logi@openhealthagents.org/alphaesai/ClaimsProcessing")
sys.path.append(str(root_dir))

from Shared.EDIProcessing import EDIProcessor, CSVConverter
from DimMember.EDIProcessing.mapper import Mapper

def move_file(src: Path, dest_dir: Path) -> Path:
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest_file = dest_dir / src.name
    shutil.move(str(src), str(dest_file))
    return dest_file

def process_single_file(pending_file: Path, base_source_dir: Path):
    active_file = pending_file
    try:
        if not active_file.exists():
            raise FileNotFoundError(f"Input file missing: {active_file}")
        
        # Pending -> Inprogress
        active_file = move_file(active_file, base_source_dir / "inprogress")

        # Pipeline Execution
        structured_json = EDIProcessor().parse(str(active_file))

        # Extract ClientID and FileID from interchange segment (ISA06 and ISA13)
        interchange = structured_json.get('interchange', {})
        client_id = interchange.get('sender_id', '').strip()
        file_id = interchange.get('control_number', '').strip()
        print(f"Extracted ClientID: {client_id}, FileID: {file_id}")
        
        mapper_data = Mapper().map_member(structured_json)
        
        # Save output CSV
        output_file = root_dir / f"temp/834/{active_file.stem}.csv"
        output_file.parent.mkdir(parents=True, exist_ok=True)
        CSVConverter().converter(mapper_data, str(output_file))
        
        # Inprogress -> Processed
        move_file(active_file, base_source_dir / "processed")
        print(f"Successfully processed: {active_file.name}")
        
    except Exception as e:
        print(f"Failed processing {active_file.name}: {e}")
        if active_file.exists():
            move_file(active_file, base_source_dir / "failed")

def main():
    base_source_dir = root_dir / "source/834"
    pending_dir = base_source_dir / "pending"
    
    print(f"Looking for files in: {pending_dir}")
    
    if not pending_dir.exists():
        print(f"Pending directory does not exist!")
        return

    pending_files = [f for f in pending_dir.iterdir() if f.is_file() and not f.name.startswith('.')]
    print(f"Found {len(pending_files)} file(s) to process.")

    for file_path in pending_files:
        process_single_file(file_path, base_source_dir)

if __name__ == "__main__":
    main()
