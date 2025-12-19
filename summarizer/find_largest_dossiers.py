#!/usr/bin/env python3
"""Find the largest dossiers in terms of total file size of their markdown files."""
from pathlib import Path
from collections import defaultdict
import sys
import re


def format_size(size_bytes: int) -> str:
    """Format size in human-readable format."""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} TB"


def extract_dossier_id_from_filename(filename: str) -> str | None:
    """Extract dossier ID from filename like '56K0191011.md'."""
    # Pattern: {session_id}K{dossier_id}{doc_id}.md
    # Try with zero-padding first: 56K0191011.md -> dossier_id = 191
    match = re.match(r'(\d+)K(\d{4})(\d{3})\.md', filename)
    if match:
        return match.group(2).lstrip('0') or '0'
    
    # Try without zero-padding: 56K19111.md
    match = re.match(r'(\d+)K(\d+)(\d+)\.md', filename)
    if match:
        # This is trickier - we need to know the format
        # For now, try to extract a reasonable dossier_id
        full_id = match.group(2) + match.group(3)
        # Common dossier IDs are 1-4 digits, so try splitting
        # This is a heuristic - might not always work
        return None
    
    return None


def main():
    base_dir = Path(__file__).parent.parent
    markdown_base_path = base_dir / "pdf-parser" / "processed_markdown"
    
    if not markdown_base_path.exists():
        print(f"Error: {markdown_base_path} not found")
        sys.exit(1)
    
    print("Scanning markdown directories...")
    
    # Calculate total size per dossier by scanning directories
    dossier_sizes = defaultdict(lambda: {'total_size': 0, 'file_count': 0})
    
    # Iterate through all dossier directories
    for dossier_dir in markdown_base_path.iterdir():
        if not dossier_dir.is_dir():
            continue
        
        dossier_id = dossier_dir.name
        
        # Count all .md files in this directory
        for md_file in dossier_dir.glob("*.md"):
            try:
                size = md_file.stat().st_size
                dossier_sizes[dossier_id]['total_size'] += size
                dossier_sizes[dossier_id]['file_count'] += 1
            except Exception as e:
                print(f"Warning: Could not read {md_file}: {e}")
    
    # Sort by total size
    sorted_dossiers = sorted(
        dossier_sizes.items(),
        key=lambda x: x[1]['total_size'],
        reverse=True
    )
    
    print(f"\n{'='*100}")
    print(f"{'Rank':<6} {'Dossier ID':<12} {'Total Size':<15} {'Files':<8}")
    print(f"{'='*100}")
    
    for rank, (dossier_id, info) in enumerate(sorted_dossiers[:50], 1):
        print(f"{rank:<6} {dossier_id:<12} {format_size(info['total_size']):<15} "
              f"{info['file_count']:<8}")
    
    print(f"\n{'='*100}")
    print(f"\nTop 20 largest dossiers:")
    for rank, (dossier_id, info) in enumerate(sorted_dossiers[:20], 1):
        print(f"{rank}. Dossier {dossier_id}: {format_size(info['total_size'])} "
              f"({info['file_count']} files)")
    
    # Calculate statistics
    total_size = sum(info['total_size'] for _, info in dossier_sizes.items())
    total_files = sum(info['file_count'] for _, info in dossier_sizes.items())
    avg_size = total_size / len(dossier_sizes) if dossier_sizes else 0
    
    print(f"\n{'='*100}")
    print(f"Statistics:")
    print(f"  Total dossiers: {len(dossier_sizes)}")
    print(f"  Total files: {total_files}")
    print(f"  Total size: {format_size(total_size)}")
    print(f"  Average size per dossier: {format_size(avg_size)}")


if __name__ == "__main__":
    main()

