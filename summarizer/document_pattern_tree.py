#!/usr/bin/env python3
"""Build a collapsible tree showing document type patterns in parliamentary dossiers."""
from pathlib import Path
from typing import Dict, List, Tuple, DefaultDict
from collections import defaultdict

try:
    import duckdb
    USE_DUCKDB = True
except ImportError:
    try:
        import pandas as pd
        USE_DUCKDB = False
    except ImportError:
        raise ImportError("Either duckdb or pandas+pyarrow must be installed")



def load_all_subdocuments(subdocuments_path: str) -> List[Tuple[str, str, str]]:
    """
    Load all subdocuments from parquet file.
    
    Returns:
        List of (dossier_id, document_id, document_type) tuples
    """
    if USE_DUCKDB:
        conn = duckdb.connect()
        query = f"""
        SELECT dossier_id, id, type, date
        FROM '{subdocuments_path}'
        ORDER BY dossier_id, date
        """
        df = conn.execute(query).df()
        conn.close()
    else:
        df = pd.read_parquet(subdocuments_path)
        df = df.sort_values(['dossier_id', 'date'])
    
    results = []
    for _, row in df.iterrows():
        dossier_id = row['dossier_id']
        doc_id = row['id']
        doc_type = row['type']
        results.append((dossier_id, doc_id, doc_type))
    
    return results


def build_pattern_tree(subdocuments: List[Tuple[str, str, str]]) -> Dict:
    """
    Build a tree structure showing document type patterns.
    
    The tree structure:
    - Level 1: All unique document types for document 001
    - Level 2: All unique document types for document 002 given the first document type
    - And so on...
    
    Returns:
        Nested dictionary representing the tree
    """
    # Group subdocuments by dossier
    dossiers: DefaultDict[str, List[Tuple[str, str]]] = defaultdict(list)
    for dossier_id, doc_id, doc_type in subdocuments:
        dossiers[dossier_id].append((doc_id, doc_type))
    
    # Sort documents within each dossier by ID (assuming numeric IDs like 001, 002, etc.)
    for dossier_id in dossiers:
        dossiers[dossier_id].sort(key=lambda x: (
            int(x[0]) if x[0].isdigit() else 999999,  # Sort numeric IDs numerically
            x[0]  # Fallback to string sort
        ))
    
    # Build pattern tree
    tree: Dict[str, Dict] = {}
    
    for dossier_id, docs in dossiers.items():
        if not docs:
            continue
        
        # Build path through tree
        current_level = tree
        
        for position, (doc_id, doc_type) in enumerate(docs, start=1):
            # Create key for this level
            level_key = f"pos{position}_{doc_type}"
            
            if level_key not in current_level:
                current_level[level_key] = {
                    'type': doc_type,
                    'position': position,
                    'children': {},
                    'count': 0,
                    'examples': []
                }
            
            current_level[level_key]['count'] += 1
            
            # Store example (limit to 5 examples per node)
            if len(current_level[level_key]['examples']) < 5:
                current_level[level_key]['examples'].append(dossier_id)
            
            # Move to next level
            current_level = current_level[level_key]['children']
    
    return tree


def tree_to_html(tree: Dict, subdocuments: List[Tuple[str, str, str]], output_path: Path) -> None:
    """Convert tree structure to HTML with collapsible tree visualization."""
    
    def render_node(node_key: str, node_data: Dict, indent: int = 0) -> str:
        """Recursively render a tree node."""
        doc_type = node_data['type']
        position = node_data['position']
        count = node_data['count']
        examples = node_data['examples']
        children = node_data['children']
        
        indent_str = "  " * indent
        node_id = f"node_{hash(node_key)}"
        
        html = f'{indent_str}<li>\n'
        html += f'{indent_str}  <div class="tree-node" onclick="toggleNode(\'{node_id}\')">\n'
        
        if children:
            html += f'{indent_str}    <span class="toggle" id="toggle_{node_id}">▶</span>\n'
        else:
            html += f'{indent_str}    <span class="toggle-empty"></span>\n'
        
        html += f'{indent_str}    <span class="doc-type">{doc_type}</span>\n'
        html += f'{indent_str}    <span class="position">(position {position})</span>\n'
        html += f'{indent_str}    <span class="count">×{count}</span>\n'
        html += f'{indent_str}  </div>\n'
        
        if examples:
            html += f'{indent_str}  <div class="examples">Examples: {", ".join(examples[:5])}</div>\n'
        
        if children:
            html += f'{indent_str}  <ul class="tree-children" id="{node_id}" style="display: none;">\n'
            for child_key, child_data in sorted(children.items(), key=lambda x: (-x[1]['count'], x[1]['type'])):
                html += render_node(child_key, child_data, indent + 1)
            html += f'{indent_str}  </ul>\n'
        
        html += f'{indent_str}</li>\n'
        return html
    
    # Build HTML
    html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Document Type Pattern Tree</title>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}
        
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
            background: #f5f5f5;
            padding: 20px;
            color: #333;
        }}
        
        .container {{
            max-width: 1200px;
            margin: 0 auto;
            background: white;
            border-radius: 8px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.1);
            padding: 30px;
        }}
        
        h1 {{
            margin-bottom: 10px;
            color: #2c3e50;
        }}
        
        .description {{
            color: #666;
            margin-bottom: 30px;
            line-height: 1.6;
        }}
        
        .tree {{
            list-style: none;
            padding-left: 0;
        }}
        
        .tree-node {{
            display: flex;
            align-items: center;
            padding: 8px 12px;
            margin: 4px 0;
            cursor: pointer;
            border-radius: 4px;
            transition: background-color 0.2s;
            user-select: none;
        }}
        
        .tree-node:hover {{
            background-color: #f0f0f0;
        }}
        
        .toggle {{
            display: inline-block;
            width: 20px;
            text-align: center;
            margin-right: 8px;
            font-size: 10px;
            color: #666;
            transition: transform 0.2s;
        }}
        
        .toggle.expanded {{
            transform: rotate(90deg);
        }}
        
        .toggle-empty {{
            display: inline-block;
            width: 20px;
            margin-right: 8px;
        }}
        
        .doc-type {{
            font-weight: 600;
            color: #2c3e50;
            margin-right: 8px;
        }}
        
        .position {{
            color: #7f8c8d;
            font-size: 0.9em;
            margin-right: 8px;
        }}
        
        .count {{
            color: #3498db;
            font-weight: 600;
            margin-left: auto;
        }}
        
        .examples {{
            font-size: 0.85em;
            color: #95a5a6;
            margin-left: 28px;
            margin-top: -4px;
            margin-bottom: 8px;
            font-style: italic;
        }}
        
        .tree-children {{
            list-style: none;
            padding-left: 20px;
            margin-left: 20px;
            border-left: 2px solid #e0e0e0;
        }}
        
        .tree li {{
            margin: 0;
        }}
        
        .stats {{
            background: #ecf0f1;
            padding: 15px;
            border-radius: 4px;
            margin-bottom: 20px;
            font-size: 0.9em;
        }}
        
        .stats strong {{
            color: #2c3e50;
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>Document Type Pattern Tree</h1>
        <div class="description">
            This tree shows the patterns of document types in parliamentary dossiers.
            Each level represents a position in the document sequence (001, 002, 003, etc.).
            Click on nodes to expand/collapse branches and explore the flows.
        </div>
"""
    
    # Calculate statistics
    total_dossiers = len(set(doc[0] for doc in subdocuments))
    total_documents = len(subdocuments)
    
    html_content += f"""
        <div class="stats">
            <strong>Statistics:</strong> {total_dossiers} dossiers, {total_documents} total documents
        </div>
        
        <ul class="tree">
"""
    
    # Render tree nodes
    for node_key, node_data in sorted(tree.items(), key=lambda x: (-x[1]['count'], x[1]['type'])):
        html_content += render_node(node_key, node_data, indent=0)
    
    html_content += """
        </ul>
    </div>
    
    <script>
        function toggleNode(nodeId) {
            const node = document.getElementById(nodeId);
            const toggle = document.getElementById('toggle_' + nodeId);
            
            if (node) {
                if (node.style.display === 'none') {
                    node.style.display = 'block';
                    if (toggle) toggle.classList.add('expanded');
                } else {
                    node.style.display = 'none';
                    if (toggle) toggle.classList.remove('expanded');
                }
            }
        }
        
        // Expand first level by default
        document.addEventListener('DOMContentLoaded', function() {
            const firstLevelNodes = document.querySelectorAll('.tree > li > ul');
            firstLevelNodes.forEach(node => {
                node.style.display = 'block';
                const toggle = node.previousElementSibling.querySelector('.toggle');
                if (toggle) toggle.classList.add('expanded');
            });
        });
    </script>
</body>
</html>
"""
    
    output_path.write_text(html_content, encoding='utf-8')
    print(f"HTML tree visualization saved to: {output_path}")


def main():
    """Main function to build and visualize document pattern tree."""
    base_dir = Path(__file__).parent.parent
    subdocuments_path = str(base_dir / "web" / "src" / "data" / "subdocuments.parquet")
    output_path = base_dir / "summarizer" / "document_pattern_tree.html"
    
    print("Loading subdocuments...")
    subdocuments = load_all_subdocuments(subdocuments_path)
    print(f"Loaded {len(subdocuments)} subdocuments from {len(set(doc[0] for doc in subdocuments))} dossiers")
    
    print("Building pattern tree...")
    tree = build_pattern_tree(subdocuments)
    
    print("Generating HTML visualization...")
    tree_to_html(tree, subdocuments, output_path)
    
    print(f"\nDone! Open {output_path} in your browser to view the tree.")


if __name__ == "__main__":
    main()

