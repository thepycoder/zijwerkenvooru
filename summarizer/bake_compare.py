#!/usr/bin/env python3
"""
Bake compare.html with all JSON results into a single self-contained HTML file.
This eliminates the need for a web server and avoids CORS issues.
"""
import json
import re
from pathlib import Path


def extract_model_name(filename):
    """Extract model name from filename like 'results_gpt-4o-mini.json'."""
    match = re.match(r'results_(.+)\.json', filename.name)
    return match.group(1) if match else filename.stem


def load_all_results(base_dir):
    """Load all results from runs directories."""
    runs_dir = base_dir / "runs"
    all_results = {}
    model_metadata = {}
    
    if not runs_dir.exists():
        print(f"Warning: Runs directory does not exist: {runs_dir}")
        return all_results, model_metadata
    
    # Find all run directories
    run_dirs = sorted(runs_dir.glob("run_*"), reverse=True)
    
    for run_dir in run_dirs:
        if not run_dir.is_dir():
            continue
        
        run_id = run_dir.name
        
        # Load run metadata if it exists
        metadata = None
        metadata_path = run_dir / "run_metadata.json"
        if metadata_path.exists():
            try:
                with open(metadata_path, 'r', encoding='utf-8') as f:
                    metadata = json.load(f)
            except Exception as e:
                print(f"Warning: Could not load metadata from {metadata_path}: {e}")
        
        # Load run summary if it exists
        summary = None
        summary_path = run_dir / "run_summary.json"
        if summary_path.exists():
            try:
                with open(summary_path, 'r', encoding='utf-8') as f:
                    summary = json.load(f)
            except Exception as e:
                print(f"Warning: Could not load summary from {summary_path}: {e}")
        
        # Load all results_*.json files
        result_files = list(run_dir.glob("results_*.json"))
        
        for result_file in result_files:
            try:
                with open(result_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                # Validate data
                if not data or not isinstance(data, dict) or len(data) == 0:
                    print(f"Warning: Empty or invalid data in {result_file}")
                    continue
                
                # Extract model name
                model_name = extract_model_name(result_file)
                model_key = f"{run_id}/{model_name}"
                
                # Store result data
                all_results[model_key] = data
                
                # Find model info from summary if available
                model_info = None
                if summary and 'model_results' in summary:
                    for mr in summary['model_results']:
                        if mr.get('success') and mr.get('output_file'):
                            # Extract filename from full path
                            output_filename = Path(mr['output_file']).name
                            if output_filename == result_file.name:
                                model_info = mr
                                break
                
                # Store metadata
                model_metadata[model_key] = {
                    'runPath': f"runs/{run_id}",
                    'metadata': metadata,
                    'modelInfo': model_info,
                    'filePath': f"runs/{run_id}/{result_file.name}",
                }
                
                print(f"Loaded: {model_key} ({len(data)} dossiers)")
                
            except Exception as e:
                print(f"Error loading {result_file}: {e}")
    
    return all_results, model_metadata


def bake_html(compare_html_path, output_path):
    """Bake all JSON data into the HTML file."""
    base_dir = compare_html_path.parent
    
    # Load the original HTML
    with open(compare_html_path, 'r', encoding='utf-8') as f:
        html_content = f.read()
    
    # Load all results
    print("Loading all results...")
    all_results, model_metadata = load_all_results(base_dir)
    
    print(f"\nLoaded {len(all_results)} model results")
    
    # Convert to JSON strings for embedding
    # We need to escape </script> tags to avoid breaking the HTML
    results_json = json.dumps(all_results, ensure_ascii=False, indent=2)
    metadata_json = json.dumps(model_metadata, ensure_ascii=False, indent=2)
    
    # Escape </script> tags to prevent breaking the HTML script tag
    results_json = results_json.replace('</script>', '<\\/script>')
    metadata_json = metadata_json.replace('</script>', '<\\/script>')
    
    # Create the embedded data section
    embedded_data = f"""        // Embedded data (baked at build time)
        let allResults = {results_json};
        let modelMetadata = {metadata_json};
        
        // Simplified loader that uses embedded data
        async function loadResults() {{
            const loadingDiv = document.getElementById('panel1-content');
            loadingDiv.innerHTML = '<div class="loading">Loading embedded data...</div>';
            
            // Data is already loaded, just populate dropdowns
            console.log(`Loaded ${{Object.keys(allResults).length}} model results from embedded data`);
            populateDropdowns();
        }}
"""
    
    # Find the section to replace: from "let allResults = {};" to end of loadResults() function
    # We need to find where extractModelName function starts (which comes after all the loading functions)
    extract_model_match = re.search(r'(\s+function extractModelName\(filename\) \{)', html_content)
    if not extract_model_match:
        raise ValueError("Could not find extractModelName function (marker for replacement)")
    
    # Find everything from "let allResults = {};" to just before extractModelName
    # This includes all the variable declarations and all the loading functions
    # Match from "let allResults" through all loading functions until extractModelName
    replacement_pattern = r'(let allResults = \{\};\s+let modelMetadata = \{[^}]*\};\s+// Structure:.*?)(\s+function extractModelName\(filename\) \{)'
    
    # Try a simpler approach: match from the variable declarations to just before extractModelName
    # Use a more flexible pattern that handles multiline comments
    replacement_pattern = r'(let allResults = \{\};\s+let modelMetadata = \{[^}]*\};\s+// Structure:.*?)(\s+function extractModelName)'
    
    def replace_loader(match):
        return embedded_data + match.group(2)
    
    result = re.sub(replacement_pattern, replace_loader, html_content, flags=re.DOTALL)
    if result == html_content:
        # Pattern didn't match, try a different approach
        # Find the position of extractModelName
        extract_pos = html_content.find('function extractModelName(filename)')
        if extract_pos == -1:
            raise ValueError("Could not find extractModelName function")
        
        # Find the position where we want to start replacing (after <script>)
        script_pos = html_content.find('<script>')
        if script_pos == -1:
            raise ValueError("Could not find <script> tag")
        
        # Find "let allResults" after the script tag
        results_pos = html_content.find('let allResults = {};', script_pos)
        if results_pos == -1:
            raise ValueError("Could not find 'let allResults = {};' declaration")
        
        # Replace everything from results_pos to extract_pos
        html_content = html_content[:results_pos] + embedded_data + html_content[extract_pos:]
    else:
        html_content = result
    
    # Write the baked HTML
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    print(f"\nBaked HTML saved to: {output_path}")
    print(f"File size: {output_path.stat().st_size / 1024 / 1024:.2f} MB")


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Bake compare.html with all JSON results into a single self-contained HTML file"
    )
    parser.add_argument(
        '--input',
        type=str,
        default='compare.html',
        help='Input HTML file (default: compare.html)'
    )
    parser.add_argument(
        '--output',
        type=str,
        default='compare_baked.html',
        help='Output HTML file (default: compare_baked.html)'
    )
    
    args = parser.parse_args()
    
    script_dir = Path(__file__).parent
    input_path = script_dir / args.input
    output_path = script_dir / args.output
    
    if not input_path.exists():
        print(f"Error: Input file does not exist: {input_path}")
        return 1
    
    try:
        bake_html(input_path, output_path)
        return 0
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit(main())

