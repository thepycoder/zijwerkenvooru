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
