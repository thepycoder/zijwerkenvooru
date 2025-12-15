import fitz
import os
import sys

# Paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DOWNLOADS_DIR = os.path.join(BASE_DIR, 'downloads')
OUTPUT_DIR = os.path.join(BASE_DIR, 'processed_markdown')

def is_header_or_footer(bbox, page_height):
    # Heuristics: Top 10% or Bottom 10% might be too aggressive/loose.
    # Based on inspection:
    # Header text was at y ~30-50.
    # Footer text was at y ~760-780 on 842 height.
    # Let's use 70 points from top and bottom.
    x0, y0, x1, y1 = bbox
    if y1 < 70:
        return True
    if y0 > page_height - 70:
        return True
    return False

def is_right_column(bbox, page_width):
    # Center of bbox
    x0, y0, x1, y1 = bbox
    center_x = (x0 + x1) / 2
    # Threshold: 52% of page width
    # This excludes elements that are clearly in the right half (French column),
    # including those close to the center like "Voir:" references (at ~55%),
    # while preserving truly centered titles (at ~50-51%).
    return center_x > (page_width * 0.52)

def span_to_markdown(span):
    text = span['text']
    # Minimal formatting
    if span['flags'] & 2**4: # bold
        text = f"**{text}**"
    if span['flags'] & 2**1: # italic
        text = f"*{text}*"
    return text

def process_page(page):
    blocks = page.get_text("dict")["blocks"]
    page_width = page.rect.width
    page_height = page.rect.height
    
    valid_blocks = []
    
    for b in blocks:
        if b['type'] != 0: # 0 = text
            continue
            
        bbox = b['bbox']
        
        # Filters
        if is_header_or_footer(bbox, page_height):
            continue
            
        if is_right_column(bbox, page_width):
            continue
            
        valid_blocks.append(b)
        
    # Sort blocks by Y (top to bottom), then X (left to right)
    # PyMuPDF usually returns them sorted, but good to ensure.
    valid_blocks.sort(key=lambda b: (b['bbox'][1], b['bbox'][0]))
    
    md_content = ""
    
    for b in valid_blocks:
        block_text = ""
        # Determine if this block looks like a header
        # We can look at the font size of the first span
        first_span_size = 0
        if b['lines'] and b['lines'][0]['spans']:
            first_span_size = b['lines'][0]['spans'][0]['size']
        
        # Heuristic for headers
        prefix = ""
        if first_span_size > 14:
            prefix = "## "
        elif first_span_size > 12:
            prefix = "### "
            
        for line in b['lines']:
            line_text = ""
            for span in line['spans']:
                line_text += span_to_markdown(span)
            block_text += line_text + " "
            
        md_content += f"{prefix}{block_text.strip()}\n\n"
        
    return md_content

def main():
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
        
    if not os.path.exists(DOWNLOADS_DIR):
        print(f"Downloads directory not found: {DOWNLOADS_DIR}")
        return

    # Iterate over dossier folders
    for dossier_id in os.listdir(DOWNLOADS_DIR):
        dossier_path = os.path.join(DOWNLOADS_DIR, dossier_id)
        if not os.path.isdir(dossier_path):
            continue
            
        print(f"Processing dossier: {dossier_id}")
        output_dossier_path = os.path.join(OUTPUT_DIR, dossier_id)
        if not os.path.exists(output_dossier_path):
            os.makedirs(output_dossier_path)
            
        # Iterate over PDFs
        for filename in os.listdir(dossier_path):
            if not filename.lower().endswith('.pdf'):
                continue
                
            pdf_path = os.path.join(dossier_path, filename)
            md_filename = filename.replace('.pdf', '.md')
            md_path = os.path.join(output_dossier_path, md_filename)
            
            # Skip if already exists? Maybe overwrite for now.
            
            try:
                doc = fitz.open(pdf_path)
                full_md = ""
                for page_num, page in enumerate(doc):
                    # full_md += f"<!-- Page {page_num + 1} -->\n\n"
                    full_md += process_page(page)
                    full_md += "\n---\n\n" # Page break
                
                with open(md_path, 'w', encoding='utf-8') as f:
                    f.write(full_md)
                    
                print(f"  Processed {filename}")
                
            except Exception as e:
                print(f"  Error processing {filename}: {e}")

if __name__ == "__main__":
    main()

