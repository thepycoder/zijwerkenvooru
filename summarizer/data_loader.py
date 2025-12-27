"""Data loading utilities for reading parquet files and markdown content."""
import os
from pathlib import Path
from typing import List, Optional

try:
    import duckdb
    USE_DUCKDB = True
except ImportError:
    try:
        import pandas as pd
        USE_DUCKDB = False
    except ImportError:
        raise ImportError("Either duckdb or pandas+pyarrow must be installed")

from .models import DocumentType, Subdocument, Dossier


def load_dossier_from_parquet(
    dossier_id: str,
    subdocuments_path: str,
    dossiers_path: str,
    markdown_base_path: str,
) -> Optional[Dossier]:
    """
    Load a dossier with all its subdocuments from parquet files.
    
    Args:
        dossier_id: The dossier ID to load
        subdocuments_path: Path to subdocuments.parquet
        dossiers_path: Path to dossiers.parquet
        markdown_base_path: Base path to markdown files (e.g., 'pdf-parser/processed_markdown')
    
    Returns:
        Dossier object with loaded subdocuments, or None if not found
    """
    # Load dossier metadata
    if USE_DUCKDB:
        conn = duckdb.connect()
        dossier_query = f"""
        SELECT * FROM '{dossiers_path}'
        WHERE id = '{dossier_id}'
        LIMIT 1
        """
        dossier_df = conn.execute(dossier_query).df()
        
        if dossier_df.empty:
            conn.close()
            return None
        
        dossier_row = dossier_df.iloc[0]
        
        # Load subdocuments
        subdocs_query = f"""
        SELECT * FROM '{subdocuments_path}'
        WHERE dossier_id = '{dossier_id}'
        ORDER BY date
        """
        subdocs_df = conn.execute(subdocs_query).df()
        conn.close()
    else:
        dossiers_df = pd.read_parquet(dossiers_path)
        dossier_df = dossiers_df[dossiers_df['id'] == dossier_id]
        
        if dossier_df.empty:
            return None
        
        dossier_row = dossier_df.iloc[0]
        
        # Load subdocuments
        subdocs_df = pd.read_parquet(subdocuments_path)
        subdocs_df = subdocs_df[subdocs_df['dossier_id'] == dossier_id]
        subdocs_df = subdocs_df.sort_values('date')
    
    # Parse authors
    dossier_authors = dossier_row['authors'].split(',') if dossier_row['authors'] else []
    
    # Convert subdocuments
    subdocuments = []
    for _, row in subdocs_df.iterrows():
        # Parse document type
        doc_type_str = row['type']
        try:
            doc_type = DocumentType(doc_type_str)
        except ValueError:
            doc_type = DocumentType.UNKNOWN
        
        # Parse authors
        authors = row['authors'].split(',') if row['authors'] else []
        authors = [a.strip() for a in authors if a.strip()]
        
        # Load markdown content
        content = load_markdown_content(
            dossier_id=row['dossier_id'],
            doc_id=row['id'],
            markdown_base_path=markdown_base_path,
        )
        
        subdoc = Subdocument(
            dossier_id=row['dossier_id'],
            id=row['id'],
            document_type=doc_type,
            date=row['date'],
            authors=authors,
            content=content,
        )
        subdocuments.append(subdoc)
    
    return Dossier(
        dossier_id=dossier_id,
        title=dossier_row['title'],
        authors=dossier_authors,
        submission_date=dossier_row['submission_date'],
        end_date=dossier_row['end_date'] if dossier_row['end_date'] else None,
        vote_date=dossier_row['vote_date'] if dossier_row['vote_date'] else None,
        document_type=dossier_row['document_type'],
        status=dossier_row['status'],
        subdocuments=subdocuments,
    )


def load_markdown_content(
    dossier_id: str,
    doc_id: str,
    markdown_base_path: str,
) -> Optional[str]:
    """
    Load markdown content for a document.
    
    The file path is expected to be: {markdown_base_path}/{dossier_id}/{session_id}K{dossier_id}{doc_id}.md
    We need to infer the session_id (usually 56 based on the codebase).
    """
    # Try session_id 56 first (most common)
    for session_id in [56, 55, 57]:  # Try common session IDs
        # Format: 56K0191011.md - Try various paddings
        # In download script: {session_id}K{dossier_id_padded}{subdoc_id}.pdf
        # dossier_id_padded is 4 chars. subdoc_id is variable.
        
        candidates = [
            f"{session_id}K{dossier_id.zfill(4)}{doc_id}.md",
            f"{session_id}K{dossier_id.zfill(4)}{doc_id.zfill(3)}.md",
            f"{session_id}K{dossier_id}{doc_id}.md"
        ]

        for filename in candidates:
             file_path = Path(markdown_base_path) / dossier_id / filename
             if file_path.exists():
                try:
                    return file_path.read_text(encoding='utf-8')
                except Exception as e:
                    print(f"Warning: Could not read {file_path}: {e}")
                    return None
    
    return None

