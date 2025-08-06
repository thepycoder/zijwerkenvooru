set windows-shell := ["C:\\Users\\AlexanderAmeye\\AppData\\Local\\Programs\\Git\\bin\\sh.exe", "-c"]

default:
    just --list

setup:
    @echo "🚀 Installing dependencies"
    npm install

run:
    (cd web && npm run dev)

scrape-members:
    rust-script "scraper/scrapers/members.rs"

scrape-lobby:
    rust-script "scraper/scrapers/lobby.rs"

scrape-remunerations:
    rust-script "scraper/scrapers/remunerations.rs"

scrape-plenary:
    cargo run --bin plenary-scraper

scrape-commission:
    cargo run --bin commission-scraper

summarize:
    cargo run --bin summarizer

post:
    cargo run --bin poster