<div align="center">

# `🗳️ zijwerkenvooru`

**How does the Belgian parliament vote?**

<img src="docs/zijwerkenvooru.png" alt="isolated" width="700"/>

</div>

## ❓ About

`https://www.zijwerkenvooru.be` is a website that aims to improve transparency and accessibility of the voting history of Belgian parliament's.

In a nutshell, the project does the following steps:

1. Scrapes HTML pages from official sources such as `https://www.dekamer.be`
2. Parses the HTML pages and extracts the relevant data
3. Transforms and stores the data in computer-friendly `.parquet` files
4. Uses the generated `.parquet` files to generate a website that showcases different views of the data (votes, questions, dossiers etc.)

## 🏗️ Architecture

This repository consist of 4 components that are linked sequentially to build the final website.

### 1. Scraper

The scraper downloads and parses data from `dekamer.be` and `public.regimand.be`. It performs the following steps:

1. Downloads different HTML pages from `dekamer.be` and `public.regimand.be`
2. Caches the html pages under `/sources` to avoid unnecessary future downloads
3. Parses the HTML pages and extracts relevant data
4. Generates `.parquet` files using the extracted data

The end result of the scraper is a set of organized `.parquet` files that contain information about parliamentary members, votes, questions, dossiers, propositions etc.

The scraper code can be found in the `scraper` directory. It contains of the following subdirectories:

- `crawl`: A rust crate that helps with scraping
- `commission-scraper`: Rust project for scraping commission meeting data
- `plenary-scraper`: Rust project for scraping plenary meeting data
- `scrapers`: Collection of single-script scrapers (members, lobby, remunerations)
- `data/sources`: Collection of downloaded HTML pages (a cache to avoid unnecessary downloads)
- `current_commission_id.txt` and `current_plenary_id.txt`: For keeping track of the latest processed plenary and commission meetings, these are automatically updated

### 2. Summarizer

The summarizer is used to enrich the output of the scraper. It uses the Mistral API to perform summarization tasks.

Currently, the summarizer is only used to summarize plenary question topics into a single topic.

The summarizer is a Rust project and can be found in the `summarizer` directory.

### 3. Website (https://www.zijwerkenvooru.be)

The generated `.parquet` files are used to statically generate a website that provides an overview of the parliamentary activities.

For this, the `11ty/eleventy` framework is used. `DuckDB` is used to query the `.parquet` files. The output is a set of HTML pages that get hosted on `Cloudflare Pages`.

The project for the website can be found in the `web` directory.

### 4. Bluesky poster

The Bluesky poster is used to automatically create posts on Bluesky showing a summary of the most recent plenary session,

The poster is a Rust project and can be found in the `poster` directory.

## 🤖 GitHub Actions

There is a single GitHub Actions workflow for automatically scraping data, summarizing data, building and deploying the website and posting. It is run on a schedule or can be triggered manually.

## 🗃️ Data

The `parquet` files are published under the `web/src/data` directory. They include the following data files:

- `commission_questions.parquet` (questions asked during commission meetings)
- `commissions.parquet` (commission meetings)
- `dossiers.parquet` (dossiers)
- `lobby.parquet` (the lobbyregister)
- `meetings.parquet` (plenary meetings)
- `members.parquet` (members of parliament)
- `propositions.parquet` (propositions from during the plenary meeting)
- `questions.parquet` (questions asked during plenary meetings)
- `remunerations.parquet` (remunerations of members of parliament)
- `subdocuments.parquet` (subdocuments linked to dossiers)
- `summaries.parquet` (AI generated summaries of topics)
- `votes.parquet` (votes that happened during plenary meetings)

### Disclaimer

1. The data is collected on a best-effort basis. Completeness and correctness can not be guaranteed.
2. The data currently only covers the current session 56 (2024-2029). The scraper is capable of scraping older data as well, but this is currently not enabled. It's planned to enable this in the near future.

### Updates

The dataset is updated daily.

The dataset is also published on `https://data.gov.be/nl/datasets/datafederaalparlement`.

## 🛠️ Contributing

Please read our [Contributor Guide](CONTRIBUTING.md) for more information on how to get started.

## ✅ Future improvements

Only bug-fixes and some ad-hoc development is done for this project. Below is a list of future improvements that can be done:

**Improvements:**

- [ ] summarize dossier contents using Mistral
- [ ] add a data link to the menu on mobile
- [ ] add parties between brackets for attendance
- [ ] check attendance calculation
- [ ] remove twitter link
- [ ] add link to data.gov dataset
- [ ] scrape previous sessions (already supported but need to make sure there are no issues with this)
- [ ] French localization
- [ ] General UI/UX improvements
- [ ] Cleanup of chart code
- [ ] Do a spot check for data correctness
- [ ] redirect https://zijwerkenvooru.be/data/ to https://zijwerkenvooru.be/nl/data/

**Features:**

- [ ] Show number of questions over time
- [ ] More interesting member overview pages

**Fixes:**

- [ ] Document 56K0095 has incorrect date (1970)
