set windows-shell := ["C:\\Users\\AlexanderAmeye\\AppData\\Local\\Programs\\Git\\bin\\sh.exe", "-c"]

default:
    just --list

setup:
    (cd web && npm install)

run:
    (cd web && npm run dev)

scrape-members:
    rust-script "scraper/scrapers/members.rs"

scrape-commissions:
    rust-script "scraper/scrapers/commissions.rs"

scrape-lobby:
    rust-script "scraper/scrapers/lobby.rs"

scrape-remunerations:
    rust-script "scraper/scrapers/remunerations.rs"

scrape-plenary:
    cargo run --bin plenary-scraper

scrape-commission:
    cargo run --bin commission-scraper

scrape-dossiers:
    cargo run --bin dossier-scraper

summarize:
    cargo run --bin summarizer

post:
    cargo run --bin poster

newsletter:
    cargo run --bin newsletter

convert-docs:
    ./pdf-parser/venv/bin/python pdf-parser/convert_pdfs.py

summarize-dossiers:
    ./pdf-parser/venv/bin/python summarizer/update_dossier_summaries.py

update-dossiers: scrape-dossiers convert-docs summarize-dossiers
