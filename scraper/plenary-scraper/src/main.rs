use std::collections::HashMap;
use arrow::array::{ArrayRef, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use chrono::{Local, NaiveDate};
use crawl::client::ScrapingClient;
use crawl::utils::clean_text;
use encoding_rs::WINDOWS_1252;
use http::StatusCode;
use itertools::EitherOrBoth;
use itertools::Itertools;
use parquet::arrow::ArrowWriter;
use regex::Regex;
use scraper::{ElementRef, Html, Selector};
use serde_json::json;
use std::error::Error;
use std::fmt;
use std::fs::{File, read_to_string};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::fs;
use tokio::fs::{create_dir_all, read_dir, remove_file, write};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Create client.
    let client = ScrapingClient::new();

    let session_id = 56;

    // Create folders.
    //let root = PathBuf::from("./web/src/data");
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("web/src/data");
    // println!("root = {:?}", root.canonicalize()?);

    let meetings_path = root.join("meetings.parquet");
    let votes_path = root.join("votes.parquet");
    let questions_path = root.join("questions.parquet");
    let propositions_path = root.join("propositions.parquet");
    let dossiers_path = root.join("dossiers.parquet");
    let subdocuments_path = root.join("subdocuments.parquet");

    if let Some(parent) = meetings_path.parent() {
        fs::create_dir_all(parent).await?;
    }
    if let Some(parent) = votes_path.parent() {
        fs::create_dir_all(parent).await?;
    }
    if let Some(parent) = questions_path.parent() {
        fs::create_dir_all(parent).await?;
    }
    if let Some(parent) = propositions_path.parent() {
        fs::create_dir_all(parent).await?;
    }
    if let Some(parent) = dossiers_path.parent() {
        fs::create_dir_all(parent).await?;
    }
    if let Some(parent) = subdocuments_path.parent() {
        fs::create_dir_all(parent).await?;
    }

    // Create parquet files.
    let meetings_file = File::create(meetings_path).unwrap();
    let meeting_schema = Arc::new(Schema::new(vec![
        Field::new("session_id", DataType::Utf8, false),
        Field::new("meeting_id", DataType::Utf8, false),
        Field::new("date", DataType::Utf8, false),
        Field::new("time_of_day", DataType::Utf8, false),
        Field::new("start_time", DataType::Utf8, false),
        Field::new("end_time", DataType::Utf8, false),
    ]));

    let votes_file = File::create(votes_path).unwrap();
    let votes_schema = Arc::new(Schema::new(vec![
        Field::new("vote_id", DataType::Utf8, false),
        Field::new("session_id", DataType::Utf8, false),
        Field::new("meeting_id", DataType::Utf8, false),
        Field::new("date", DataType::Utf8, false),
        Field::new("title_nl", DataType::Utf8, false),
        Field::new("title_fr", DataType::Utf8, false),
        Field::new("yes", DataType::Utf8, false),
        Field::new("no", DataType::Utf8, false),
        Field::new("abstain", DataType::Utf8, false),
        Field::new("members_yes", DataType::Utf8, false),
        Field::new("members_no", DataType::Utf8, false),
        Field::new("members_abstain", DataType::Utf8, false),
        Field::new("dossier_id", DataType::Utf8, false),
        Field::new("document_id", DataType::Utf8, false),
        Field::new("motion_id", DataType::Utf8, false),
    ]));

    let questions_file = File::create(questions_path).unwrap();
    let questions_schema = Arc::new(Schema::new(vec![
        Field::new("question_id", DataType::Utf8, false),
        Field::new("session_id", DataType::Utf8, false),
        Field::new("meeting_id", DataType::Utf8, false),
        Field::new("questioners", DataType::Utf8, false),
        Field::new("respondents", DataType::Utf8, false),
        Field::new("topics_nl", DataType::Utf8, false),
        Field::new("topics_fr", DataType::Utf8, false),
        Field::new("discussion", DataType::Utf8, false),
        Field::new("dossier_ids", DataType::Utf8, false),
    ]));

    let propositions_file = File::create(propositions_path).unwrap();
    let propositions_schema = Arc::new(Schema::new(vec![
        Field::new("proposition_id", DataType::Utf8, false),
        Field::new("session_id", DataType::Utf8, false),
        Field::new("meeting_id", DataType::Utf8, false),
        Field::new("title_nl", DataType::Utf8, false),
        Field::new("title_fr", DataType::Utf8, false),
        Field::new("dossier_id", DataType::Utf8, false),
        Field::new("document_id", DataType::Utf8, false),
    ]));

    // Scrape data.
    let mut meeting_ids = vec![];
    let mut meeting_session_ids = vec![];
    let mut dates = vec![];
    let mut times_of_day = vec![];
    let mut start_times = vec![];
    let mut end_times = vec![];
    let mut vote_ids = vec![];
    let mut vote_session_ids = vec![];
    let mut vote_meeting_ids = vec![];
    let mut vote_dates = vec![];
    let mut vote_titles_nl = vec![];
    let mut vote_titles_fr = vec![];
    let mut vote_yes = vec![];
    let mut vote_no = vec![];
    let mut vote_abstain = vec![];
    let mut vote_members_yes = vec![];
    let mut vote_members_no = vec![];
    let mut vote_members_abstain = vec![];
    let mut vote_dossier_ids = vec![];
    let mut vote_document_ids = vec![];
    let mut vote_motion_ids = vec![];
    let mut question_ids = vec![];
    let mut question_session_ids = vec![];
    let mut question_meeting_ids = vec![];
    let mut question_questioners = vec![];
    let mut question_respondents = vec![];
    let mut question_topics_nl = vec![];
    let mut question_topics_fr = vec![];
    let mut question_discussions = vec![];
    let mut question_dossier_ids = vec![];
    let mut proposition_ids = vec![];
    let mut proposition_session_ids = vec![];
    let mut proposition_meeting_ids = vec![];
    let mut proposition_titles_nl = vec![];
    let mut proposition_titles_fr = vec![];
    let mut proposition_dossier_ids = vec![];
    let mut proposition_document_ids = vec![];

    let mut web_request_count = 0;
    // println!("OK");
    let meeting_id_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("current_plenary_id.txt");

    let current_meeting_id: u32 = std::fs::read_to_string(&meeting_id_path)?.trim().parse()?;

    // let current_meeting_id: u32 = std::fs::read_to_string("../../current_meeting_id.txt")?.trim().parse()?;
    // println!("NOK");

    let mut last_meeting_id = current_meeting_id;

    loop {
        let probe = last_meeting_id + 1;
        let url = format!(
            "https://www.dekamer.be/doc/PCRI/html/{}/ip{:03}x.html",
            session_id, probe
        );
        let resp = client.get(&url).await?;
        web_request_count += 1;

        if resp.status() == StatusCode::NOT_FOUND {
            break;
        } else {
            // found a new one, move forward
            last_meeting_id = probe;
        }
    }

    if last_meeting_id == current_meeting_id {
        println!("No new meeting available.");
    } else {
        println!("Found new meetings up to {}", last_meeting_id);
    }

    for meeting in 1..=last_meeting_id {
        scrape_meeting(
            &client,
            56,
            meeting,
            &mut meeting_ids,
            &mut meeting_session_ids,
            &mut dates,
            &mut times_of_day,
            &mut start_times,
            &mut end_times,
            &mut vote_ids,
            &mut vote_session_ids,
            &mut vote_meeting_ids,
            &mut vote_dates,
            &mut vote_titles_nl,
            &mut vote_titles_fr,
            &mut vote_yes,
            &mut vote_no,
            &mut vote_abstain,
            &mut vote_members_yes,
            &mut vote_members_no,
            &mut vote_members_abstain,
            &mut vote_dossier_ids,
            &mut vote_document_ids,
            &mut vote_motion_ids,
            &mut question_ids,
            &mut question_session_ids,
            &mut question_meeting_ids,
            &mut question_questioners,
            &mut question_respondents,
            &mut question_discussions,
            &mut question_topics_nl,
            &mut question_topics_fr,
            &mut question_dossier_ids,
            &mut proposition_ids,
            &mut proposition_session_ids,
            &mut proposition_meeting_ids,
            &mut proposition_titles_nl,
            &mut proposition_titles_fr,
            &mut proposition_dossier_ids,
            &mut proposition_document_ids,
            &mut web_request_count,
        )
        .await?;
    }

    std::fs::write(&meeting_id_path, last_meeting_id.to_string())?;

    let meetings_batch = RecordBatch::try_new(
        meeting_schema.clone(),
        vec![
            Arc::new(StringArray::from(meeting_session_ids)) as ArrayRef,
            Arc::new(StringArray::from(meeting_ids)),
            Arc::new(StringArray::from(dates)),
            Arc::new(StringArray::from(times_of_day)),
            Arc::new(StringArray::from(start_times)),
            Arc::new(StringArray::from(end_times)),
        ],
    )?;

    let mut meetings = ArrowWriter::try_new(meetings_file, meeting_schema, None)?;
    meetings.write(&meetings_batch)?;
    meetings.close().unwrap();

    let votes_batch = RecordBatch::try_new(
        votes_schema.clone(),
        vec![
            Arc::new(StringArray::from(vote_ids)) as ArrayRef,
            Arc::new(StringArray::from(vote_session_ids)),
            Arc::new(StringArray::from(vote_meeting_ids)),
            Arc::new(StringArray::from(vote_dates)),
            Arc::new(StringArray::from(vote_titles_nl)),
            Arc::new(StringArray::from(vote_titles_fr)),
            Arc::new(StringArray::from(vote_yes)),
            Arc::new(StringArray::from(vote_no)),
            Arc::new(StringArray::from(vote_abstain)),
            Arc::new(StringArray::from(vote_members_yes)),
            Arc::new(StringArray::from(vote_members_no)),
            Arc::new(StringArray::from(vote_members_abstain)),
            Arc::new(StringArray::from(vote_dossier_ids)),
            Arc::new(StringArray::from(vote_document_ids)),
            Arc::new(StringArray::from(vote_motion_ids)),
        ],
    )?;

    let mut votes = ArrowWriter::try_new(votes_file, votes_schema, None)?;
    votes.write(&votes_batch)?;
    votes.close().unwrap();

    let questions_batch = RecordBatch::try_new(
        questions_schema.clone(),
        vec![
            Arc::new(StringArray::from(question_ids)) as ArrayRef,
            Arc::new(StringArray::from(question_session_ids)),
            Arc::new(StringArray::from(question_meeting_ids)),
            Arc::new(StringArray::from(question_questioners)),
            Arc::new(StringArray::from(question_respondents)),
            Arc::new(StringArray::from(question_topics_nl)),
            Arc::new(StringArray::from(question_topics_fr)),
            Arc::new(StringArray::from(question_discussions)),
            Arc::new(StringArray::from(question_dossier_ids)),
        ],
    )?;

    let mut questions = ArrowWriter::try_new(questions_file, questions_schema, None)?;
    questions.write(&questions_batch)?;
    questions.close().unwrap();

    let propositions_batch = RecordBatch::try_new(
        propositions_schema.clone(),
        vec![
            Arc::new(StringArray::from(proposition_ids)) as ArrayRef,
            Arc::new(StringArray::from(proposition_session_ids)),
            Arc::new(StringArray::from(proposition_meeting_ids)),
            Arc::new(StringArray::from(proposition_titles_nl)),
            Arc::new(StringArray::from(proposition_titles_fr)),
            Arc::new(StringArray::from(proposition_dossier_ids)),
            Arc::new(StringArray::from(proposition_document_ids)),
        ],
    )?;

    let mut propositions = ArrowWriter::try_new(propositions_file, propositions_schema, None)?;
    propositions.write(&propositions_batch)?;
    propositions.close().unwrap();

    // Scrape dossiers.
    let mut dossier_session_ids = vec![];
    let mut dossier_ids = vec![];
    let mut dossier_titles = vec![];
    let mut dossier_authors = vec![];
    let mut dossier_submission_dates = vec![];
    let mut dossier_end_dates = vec![];
    let mut dossier_vote_dates = vec![];
    let mut dossier_document_types = vec![];
    let mut dossier_statuses = vec![];

    let mut subdocument_dossier_ids = vec![];
    let mut subdocument_ids = vec![];
    let mut subdocument_dates = vec![];
    let mut subdocument_types = vec![];
    let mut subdocument_authors = vec![];

    let dossier_root = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
    let foldername = format!("data/sources/sessions/{}/dossiers", session_id);
    let dossier_dir = dossier_root.join(foldername);

    //  let dossier_dir = "scraper/data/sources/dossiers";
    let mut entries = read_dir(dossier_dir).await?;
    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) == Some("html") {
            let path_str = path.to_string_lossy().to_string();

            let dossier_id = path
                .file_stem()
                .and_then(|s| s.to_str())
                .and_then(|s| s.split('_').nth(1)) // get the second part
                .unwrap_or("")
                .to_string();

            let content = match read_to_string(&path_str) {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("Failed to read {}: {}", path_str, e);
                    continue;
                }
            };
            let document = Html::parse_document(&content);
            let dossier = scrape_dossier(dossier_id.as_str(), &document).await?;

            dossier_session_ids.push(session_id.to_string());
            dossier_ids.push(dossier_id);
            dossier_titles.push(dossier.title);
            dossier_authors.push(dossier.authors.join(",").to_string());
            dossier_submission_dates.push(dossier.submission_date);
            dossier_end_dates.push(dossier.end_date);
            dossier_vote_dates.push(dossier.vote_date);
            dossier_document_types.push(dossier.document_type.to_string());
            dossier_statuses.push(dossier.status.to_string());

            // Parse subdocuments.
            for subdocument in dossier.subdocuments {
                subdocument_dossier_ids.push(subdocument.dossier_id);
                subdocument_ids.push(subdocument.id);
                subdocument_dates.push(subdocument.date);
                subdocument_types.push(subdocument.document_type.to_string());
                subdocument_authors.push(subdocument.authors.join(",").to_string());
            }
        }
    }

    let subdocuments_file = File::create(subdocuments_path).unwrap();
    let subdocuments_schema = Arc::new(Schema::new(vec![
        Field::new("dossier_id", DataType::Utf8, false),
        Field::new("id", DataType::Utf8, false),
        Field::new("date", DataType::Utf8, false),
        Field::new("type", DataType::Utf8, false),
        Field::new("authors", DataType::Utf8, false),
    ]));
    let subdocuments_batch = RecordBatch::try_new(
        subdocuments_schema.clone(),
        vec![
            Arc::new(StringArray::from(subdocument_dossier_ids)),
            Arc::new(StringArray::from(subdocument_ids)),
            Arc::new(StringArray::from(subdocument_dates)),
            Arc::new(StringArray::from(subdocument_types)),
            Arc::new(StringArray::from(subdocument_authors)),
        ],
    )?;
    let mut subdocuments = ArrowWriter::try_new(subdocuments_file, subdocuments_schema, None)?;
    subdocuments.write(&subdocuments_batch)?;
    subdocuments.close().unwrap();

    let dossiers_file = File::create(dossiers_path).unwrap();
    let dossiers_schema = Arc::new(Schema::new(vec![
        Field::new("session_id", DataType::Utf8, false),
        Field::new("id", DataType::Utf8, false),
        Field::new("title", DataType::Utf8, false),
        Field::new("authors", DataType::Utf8, false),
        Field::new("submission_date", DataType::Utf8, false),
        Field::new("end_date", DataType::Utf8, false),
        Field::new("vote_date", DataType::Utf8, false),
        Field::new("document_type", DataType::Utf8, false),
        Field::new("status", DataType::Utf8, false),
    ]));
    let dossiers_batch = RecordBatch::try_new(
        dossiers_schema.clone(),
        vec![
            Arc::new(StringArray::from(dossier_session_ids)),
            Arc::new(StringArray::from(dossier_ids)),
            Arc::new(StringArray::from(dossier_titles)),
            Arc::new(StringArray::from(dossier_authors)),
            Arc::new(StringArray::from(dossier_submission_dates)),
            Arc::new(StringArray::from(dossier_end_dates)),
            Arc::new(StringArray::from(dossier_vote_dates)),
            Arc::new(StringArray::from(dossier_document_types)),
            Arc::new(StringArray::from(dossier_statuses)),
        ],
    )?;
    let mut dossiers = ArrowWriter::try_new(dossiers_file, dossiers_schema, None)?;
    dossiers.write(&dossiers_batch)?;
    dossiers.close().unwrap();

    println!("Scraped data using {} web request(s).", web_request_count);

    Ok(())
}

#[derive(Debug, Clone, Copy)]
enum DocumentStatus {
    ZonderVoorwerp, // 14
    Verworpen,      // 15
    Aangenomen,     // 16
    Onbekend,
}

impl fmt::Display for DocumentStatus {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(Debug, Clone, Copy)]
enum DocumentType {
    AangenomenTekst,
    Amendement,
    Advies,
    AdviesVanDeRaadVanState,
    Verslag,
    WetsOntwerp,                 // 02
    OvergezondenOntwerp,         // 03
    WetsVoorstel,                // 05
    VoorstelVanResolutie,        // 06
    VoorstelTotHerziening,       // 08
    VoorstelOnderzoekscommissie, // 20
    VoorstelReglement,           // 21
    ArtikelenBijEersteStemmingAangenomen,
    Unknown,
}

impl fmt::Display for DocumentType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

struct Subdocument {
    dossier_id: String,
    id: String,
    document_type: DocumentType,
    date: String,
    authors: Vec<String>,
}

struct Dossier {
    title: String,
    authors: Vec<String>,
    submission_date: String,
    end_date: String,
    vote_date: String,
    document_type: DocumentType,
    status: DocumentStatus,
    subdocuments: Vec<Subdocument>,
}

fn parse_status(raw: &str) -> DocumentStatus {
    let raw = raw.trim().to_lowercase();
    if raw.contains("aangenomen") {
        DocumentStatus::Aangenomen
    } else if raw.contains("verworpen") {
        DocumentStatus::Verworpen
    } else if raw.contains("zonder voorwerp") {
        DocumentStatus::ZonderVoorwerp
    } else {
        DocumentStatus::Onbekend
    }
}

fn parse_document_type(raw: &str) -> DocumentType {
    let raw = raw.trim().to_lowercase();
    if raw.contains("voorstel van resolutie") {
        DocumentType::VoorstelVanResolutie
    } else if raw.contains("amendement") {
        DocumentType::Amendement
    } else if raw.contains("voorstel tot herziening") {
        DocumentType::VoorstelTotHerziening
    } else if raw.contains("wetsvoorstel") {
        DocumentType::WetsVoorstel
    } else if raw.contains("wetsontwerp") {
        DocumentType::WetsOntwerp
    } else if raw.contains("overgezonden ontwerp") {
        DocumentType::OvergezondenOntwerp
    }

    else if raw.contains("verslag") {
        DocumentType::Verslag
    } else if raw.contains("advies van de raad van state") {
        DocumentType::AdviesVanDeRaadVanState
    } else if raw.contains("advies") {
        DocumentType::Advies
    } else if raw.contains("voorstel onderzoekscommissie") {
        DocumentType::VoorstelOnderzoekscommissie
    } else if raw.contains("voorstel reglement") {
        DocumentType::VoorstelReglement
    } else if raw.contains("artikelen bij 1e stemming aangenomen") {
        DocumentType::ArtikelenBijEersteStemmingAangenomen
    } else if raw.contains("aangenomen tekst") {
        DocumentType::AangenomenTekst
    } else {
        DocumentType::Unknown
    }
}

async fn scrape_dossier(dossier_id: &str, document: &Html) -> Result<Dossier, Box<dyn Error>> {
    // Selectors.
    let title_selector = Selector::parse("#story h4 center").unwrap();
    let cell_selector = Selector::parse("td").unwrap();

    let title = document
        .select(&title_selector)
        .next()
        .and_then(|el| el.text().next())
        .unwrap_or("")
        .trim()
        .to_string();
    let mut submission_date = String::new();
    let mut vote_date = String::new();
    let mut end_date = String::new();
    let mut dossier_authors = Vec::new();
    let mut document_type = DocumentType::Unknown;
    let mut status = DocumentStatus::Onbekend;
    let mut subdocuments = Vec::new();

    let document_table = document
        .select(&Selector::parse("table").unwrap())
        .next()
        .unwrap();

    if let Some(tbody) = document_table
        .select(&Selector::parse("tbody").unwrap())
        .next()
    {
        for (_i, row) in document_table
            .select(&Selector::parse("tr").unwrap())
            .enumerate()
        {
            if row.parent().unwrap() == *tbody {
                let mut cells = row.select(&cell_selector);
                let cell_1 = cells.next();
                let cell_2 = cells.next();

                if let (Some(cell_1), Some(cell_2)) = (cell_1, cell_2) {
                    let label = cell_1
                        .text()
                        .collect::<String>()
                        .to_lowercase()
                        .trim()
                        .to_string();
                    let value = cell_2
                        .text()
                        .collect::<String>()
                        .to_lowercase()
                        .trim()
                        .to_string();

                    // Submission date.
                    if label.contains("indieningsdatum") {
                        submission_date = value;
                    }
                    // Vote date.
                    else if label.contains("stemming kamer") {
                        vote_date = value;
                    }
                    // End date.
                    else if label.contains("einddatum") {
                        end_date = value;
                    }
                    // Author section start
                    else if label.contains("auteur(s)") {
                        for link in cell_2.select(&Selector::parse("a").unwrap()) {
                            if let Some(name_raw) = link.text().next() {
                                // Optional: reformat "Last, First" to "First Last"
                                let name_clean = name_raw.trim();
                                let name = name_clean.replace(",", ""); // or better: custom logic if order matters
                                dossier_authors.push(name);
                            }
                        }

                        // Fallback: if no links found, try raw text
                        if dossier_authors.is_empty() {
                            for text_node in cell_2.text() {
                                let name = text_node.trim();
                                if !name.is_empty() {
                                    dossier_authors.push(name.to_string());
                                }
                            }
                        }
                    }
                    // Type.
                    else if label.contains("document type") {
                        document_type = parse_document_type(&value);
                    }
                    // Status.
                    else if label.contains("status") {
                        status = parse_status(&value);
                    }
                    // Subdocuments.
                    else if label.contains("subdocumenten") {
                        let subdocument_table = cell_2
                            .select(&Selector::parse("table").unwrap())
                            .next()
                            .unwrap();

                        let mut document_id = String::new();
                        let mut document_type: DocumentType = DocumentType::Unknown;
                        let mut document_date = String::new();
                        let mut document_authors: Vec<String> = Vec::new();

                        let mut parsing_authors = false;
                        let mut complete_subdocument = false;

                        for (_i, row) in subdocument_table
                            .select(&Selector::parse("tr").unwrap())
                            .enumerate()
                        {
                            let mut cells = row.select(&cell_selector);
                            let cell_1 = cells.next();
                            let cell_2 = cells.next();

                            // If cell 2 is empty, that's the end of the subdocument.
                            if cell_2.is_none() {
                                if complete_subdocument {
                                    subdocuments.push(Subdocument {
                                        dossier_id: dossier_id.to_string(),
                                        id: document_id.clone(),
                                        document_type: document_type.clone(),
                                        date: document_date.to_string(),
                                        authors: document_authors.clone(),
                                    });

                                    // Reset all fields for next subdocument
                                    document_id.clear();
                                    document_date.clear();
                                    document_authors.clear();
                                    complete_subdocument = false;
                                    parsing_authors = false;
                                }
                                continue;
                            }

                            if let (Some(cell_1), Some(cell_2)) = (cell_1, cell_2) {
                                let label = cell_1
                                    .text()
                                    .collect::<String>()
                                    .to_lowercase()
                                    .trim()
                                    .to_string();
                                let value = cell_2
                                    .text()
                                    .collect::<String>()
                                    .to_lowercase()
                                    .trim()
                                    .to_string();

                                // Document id.
                                if let Some(link) =
                                    cell_1.select(&Selector::parse("a").unwrap()).last()
                                {
                                    if let Some(id_text) = link.text().next() {
                                        document_id = id_text.trim().to_string();
                                    }
                                }

                                // Document type.
                                if let Some(font) =
                                    cell_2.select(&Selector::parse("font").unwrap()).next()
                                {
                                    let type_text =
                                        font.text().collect::<String>().trim().to_string();
                                    document_type = parse_document_type(&type_text);
                                }

                                // Date.
                                if label.contains("datum ronddeling") {
                                    document_date = value;
                                }

                                // Author section start
                                if label.contains("auteur(s)") {
                                    parsing_authors = true;
                                }

                                // Collect subdocument author if in author mode
                                if parsing_authors {
                                    if let Some(link) =
                                        cell_2.select(&Selector::parse("a").unwrap()).next()
                                    {
                                        if let Some(name) = link.text().next() {
                                            document_authors
                                                .push(name.replace(",", "").trim().to_string());
                                        }
                                    }
                                }

                                // Ready to commit subdocument when key fields are filled
                                if !document_id.is_empty() && !document_date.is_empty() {
                                    complete_subdocument = true;
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    Ok(Dossier {
        title,
        authors: dossier_authors,
        submission_date,
        end_date,
        vote_date,
        document_type,
        status,
        subdocuments,
    })
}

async fn scrape_meeting(
    client: &ScrapingClient,
    session_id: u32,
    meeting_id: u32,
    meeting_ids: &mut Vec<String>,
    meeting_session_ids: &mut Vec<String>,
    dates: &mut Vec<String>,
    times_of_day: &mut Vec<String>,
    start_times: &mut Vec<String>,
    end_times: &mut Vec<String>,
    vote_ids: &mut Vec<String>,
    vote_session_ids: &mut Vec<String>,
    vote_meeting_ids: &mut Vec<String>,
    vote_dates: &mut Vec<String>,
    vote_titles_nl: &mut Vec<String>,
    vote_titles_fr: &mut Vec<String>,
    vote_yes: &mut Vec<String>,
    vote_no: &mut Vec<String>,
    vote_abstain: &mut Vec<String>,
    vote_members_yes: &mut Vec<String>,
    vote_members_no: &mut Vec<String>,
    vote_members_abstain: &mut Vec<String>,
    vote_dossier_ids: &mut Vec<String>,
    vote_document_ids: &mut Vec<String>,
    vote_motion_ids: &mut Vec<String>,
    question_ids: &mut Vec<String>,
    question_session_ids: &mut Vec<String>,
    question_meeting_ids: &mut Vec<String>,
    question_questioners: &mut Vec<String>,
    question_respondents: &mut Vec<String>,
    question_discussions: &mut Vec<String>,
    question_topics_nl: &mut Vec<String>,
    question_topics_fr: &mut Vec<String>,
    question_dossier_ids: &mut Vec<String>,
    proposition_ids: &mut Vec<String>,
    proposition_session_ids: &mut Vec<String>,
    proposition_meeting_ids: &mut Vec<String>,
    proposition_titles_nl: &mut Vec<String>,
    proposition_titles_fr: &mut Vec<String>,
    proposition_dossier_ids: &mut Vec<String>,
    proposition_document_ids: &mut Vec<String>,
    web_request_count: &mut u32,
) -> Result<(), Box<dyn Error>> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
    let filename = format!("data/sources/sessions/{}/meetings/plenary/{}-{}.html", session_id, session_id, meeting_id);
    let filepath = root.join(filename);

    // Check if file already exists.
    //  let filename = format!("scraper/data/sources/meetings/{}-{}.html", session_id, meeting_id);
    // let filepath = Path::new(&filename);

    // Download meeting html if it does not exist yet.
    if !filepath.exists() {
        let url = format!(
            "https://www.dekamer.be/doc/PCRI/html/{}/ip{:03}x.html",
            session_id, meeting_id
        );

        // Fetch and parse the HTML content.
        let response = client.get(&url).await?;
        *web_request_count += 1;
        let raw_bytes = response.bytes().await?;
        let (decoded_str, _, _) = WINDOWS_1252.decode(&raw_bytes); // Decode data using windows_1252 (see meta tag of site <meta http-equiv="Content-Type" content="text/html; charset=windows-1252">)
        let content = decoded_str.to_string();
        if let Some(parent) = filepath.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(&filepath, content).unwrap();
    }

    // Fetch and parse the HTML content.
    let content = read_to_string(filepath)?;
    let document = Html::parse_document(&content);

    // Extract meeting data.
    let date = extract_date_from_document(&document)?;
    let time_of_day = extract_time_of_day_from_document(&document)?;
    let start_time = extract_start_time_from_document(&document)?;
    let end_time = extract_end_time_from_document(&document)?;

    // Selectors.
    let span_selector = Selector::parse("span").unwrap();

    // Typo map added to fix some typos that they made in the report,
    let mut typo_map = HashMap::new();
    typo_map.insert("Steven Coengrachts".to_string(), "Steven Coenegrachts".to_string());
    typo_map.insert("Ridouhane Chahid".to_string(), "Ridouane Chahid".to_string());

    // Extract questions.
    let mut previous_question_nl = String::new();
    let mut previous_question_fr = String::new();
    let mut previous_discussion = String::new();
    let mut question_id: i32 = 0;
    let mut found_questions = false;
    let mut processing_questions = false;
    // let mut collecting_subquestions = false;

    for element in document.select(&Selector::parse("h1, h2, p").unwrap()) {
        let tag_name = element.value().name();

        if tag_name == "h1" {
            let text = element
                .text()
                .collect::<Vec<_>>()
                .join(" ")
                .replace("\n", " ")
                .trim()
                .to_string();

            // Detect start of questions section.
            if text.contains("Mondelinge vragen")
                || text.contains("Vragen")
                || text.contains("Questions orales")
                || text.contains("Questions")
            {
                found_questions = true;
                processing_questions = true;
            } else if found_questions {
                // Avoid breaking on the second question header (which is just a translation).
                let is_question_header = text.contains("Mondelinge vragen")
                    || text.contains("Vragen")
                    || text.contains("Questions orales")
                    || text.contains("Questions");

                if !is_question_header {
                    // Save the grouped question.
                    if !previous_question_nl.is_empty() || !previous_question_fr.is_empty() {
                        let question_nl = extract_question_data(
                            typo_map.clone(),
                            previous_question_nl.clone(),
                            previous_discussion.clone(),
                        )
                        .await?;
                        let question_fr = extract_question_data(
                            typo_map.clone(),
                            previous_question_fr.clone(),
                            previous_discussion.clone(),
                        )
                        .await?;

                        question_ids.push(question_id.to_string());
                        question_meeting_ids.push(meeting_id.to_string());
                        question_session_ids.push(session_id.to_string());
                        question_discussions.push(question_nl.discussion.clone());
                        question_questioners.push(question_nl.questioners.join(","));
                        question_respondents.push(question_nl.respondents.join(","));
                        question_topics_nl.push(question_nl.topics.join(";"));
                        question_topics_fr.push(question_fr.topics.join(";"));
                        question_dossier_ids.push(question_nl.dossier_ids.join(","));
                        // question_id += 1;
                        previous_discussion.clear();
                        previous_question_nl.clear();
                        previous_question_fr.clear();
                    }
                    break;
                }
            }
        }

        if !processing_questions {
            continue;
        }

        // Question.
        if tag_name == "h2" {
            // NOTE: Here we extract dutch/french language parts HOWEVER, they are not 100% reliable and may be incorrect.
            // NOTE: So, we analyze the text to be sure as well.
            // NOTE: For SOME REASON in plenary meeting 71 they use NL-BE instead of NL
            let dutch_spans: Vec<_> = element
                .select(&span_selector)
                .filter(|s| matches!(s.value().attr("lang"), Some("NL") | Some("NL-BE")))
                .collect();

            let french_spans: Vec<_> = element
                .select(&span_selector)
                .filter(|s| s.value().attr("lang") == Some("FR"))
                .collect();

            let mut found_text_nl = None;
            let mut found_text_fr = None;

            let french_indicator_words = ["questions jointes"];
            let dutch_indicator_words = ["samengevoegde vragen"];

            if let Some(span) = dutch_spans.last() {
                let raw_text = span.text().collect::<Vec<_>>().join(" ");

                // NOTE: Why is this replacement done?
                let cleaned = clean_text(&raw_text);//.replace("\"", "\'");

                let is_likely_french = french_indicator_words
                    .iter()
                    .any(|word| cleaned.to_lowercase().contains(word));

                if is_likely_french {
                    found_text_fr = Some(cleaned);
                } else {
                    found_text_nl = Some(cleaned);
                }
            }

            if let Some(span) = french_spans.last() {
                let raw_text = span.text().collect::<Vec<_>>().join(" ");
                let cleaned = clean_text(&raw_text).replace("\"", "\'");

                let is_likely_dutch = dutch_indicator_words
                    .iter()
                    .any(|word| cleaned.to_lowercase().contains(word));

                if is_likely_dutch {
                    found_text_nl = Some(cleaned); // Misclassified as FR
                } else {
                    found_text_fr = Some(cleaned);
                }
            }

            // Combine decision-making logic based on Dutch/French headers
            let is_group_start = found_text_nl
                .as_deref()
                .map_or(false, |t| t.starts_with("Samengevoegde"))
                || found_text_fr
                    .as_deref()
                    .map_or(false, |t| t.contains("jointes"));

            let is_subquestion = found_text_nl
                .as_deref()
                .map_or(false, |t| t.starts_with("-"))
                || found_text_fr
                    .as_deref()
                    .map_or(false, |t| t.starts_with("-"));

            let is_single_question = found_text_nl
                .as_deref()
                .map_or(false, |t| t.starts_with("Vraag van"))
                || found_text_fr
                    .as_deref()
                    .map_or(false, |t| t.starts_with("Question de"));

            if is_group_start || is_single_question {
                if !previous_question_nl.is_empty() && !previous_question_fr.is_empty() {
                    let question_nl = extract_question_data(
                        typo_map.clone(),
                        previous_question_nl.clone(),
                        previous_discussion.clone(),
                    )
                    .await?;
                    let question_fr = extract_question_data(
                        typo_map.clone(),
                        previous_question_fr.clone(),
                        previous_discussion.clone(),
                    )
                    .await?;

                    question_ids.push(question_id.to_string());
                    question_meeting_ids.push(meeting_id.to_string());
                    question_session_ids.push(session_id.to_string());
                    question_discussions.push(question_nl.discussion.clone());
                    question_questioners.push(question_nl.questioners.join(",")); // NL used here
                    question_respondents.push(question_nl.respondents.join(","));
                    question_topics_nl.push(question_nl.topics.join(";"));
                    question_topics_fr.push(question_fr.topics.join(";"));
                    question_dossier_ids.push(question_nl.dossier_ids.join(","));

                    question_id += 1;
                    previous_discussion.clear();
                    previous_question_nl.clear();
                    previous_question_fr.clear();
                }

                // Start new grouped question.
                if let Some(text) = found_text_nl {
                    previous_question_nl = text;
                }
                if let Some(text) = found_text_fr {
                    previous_question_fr = text;
                }
                // collecting_subquestions = true;
            } else if is_subquestion {
                if let Some(text) = found_text_nl {
                    previous_question_nl.push_str("\n");
                    previous_question_nl.push_str(text.as_str());
                }
                if let Some(text) = found_text_fr {
                    previous_question_fr.push_str("\n");
                    previous_question_fr.push_str(text.as_str());
                }
            }
        }

        if tag_name == "p" {
            let paragraph_text = element
                .text()
                .collect::<Vec<_>>()
                .join(" ")
                .trim()
                .to_string();

            let discussion_paragraph = clean_text(&paragraph_text);

            if !paragraph_text.is_empty() {
                previous_discussion.push_str(discussion_paragraph.as_str());
                previous_discussion.push_str("NEWPARAGRAPH");
            }
        }
    }

    // Extract propositions + related dossiers.
    let mut previous_proposition_nl = String::new();
    let mut previous_proposition_fr = String::new();
    let mut proposition_id: i32 = 0;
    let mut found_propositions = false;
    let mut processing_propositions = false;

    for element in document.select(&Selector::parse("h1, h2").unwrap()) {
        let tag_name = element.value().name();

        if tag_name == "h1" {
            let text = element
                .text()
                .collect::<Vec<_>>()
                .join(" ")
                .replace("\n", " ")
                .trim()
                .to_string();

            let proposition_keywords_nl = ["voorstel", "wetsvoorstel"];
            let proposition_keywords_fr = ["proposition"];
            if proposition_keywords_nl
                .iter()
                .any(|&keyword| text.to_lowercase().contains(keyword))
            {
                found_propositions = true;
                processing_propositions = true;
            } else if found_propositions
                && !proposition_keywords_fr
                    .iter()
                    .any(|&keyword| text.to_lowercase().contains(keyword))
            {
                // skip french translation of the header
                if !previous_proposition_nl.is_empty() || !previous_proposition_fr.is_empty() {
                    // Flush proposition
                    let proposition_nl =
                        extract_proposition_data(previous_proposition_nl.clone()).await?;
                    let proposition_fr =
                        extract_proposition_data(previous_proposition_fr.clone()).await?;
                    let has_dossier_id = proposition_nl.dossier_id.is_some();
                    let dossier_id_opt = proposition_nl.dossier_id.clone();

                    proposition_ids.push(proposition_id.to_string());
                    proposition_meeting_ids.push(meeting_id.to_string());
                    proposition_session_ids.push(session_id.to_string());

                    proposition_titles_nl.push(proposition_nl.topic.clone());
                    proposition_titles_fr.push(proposition_fr.topic.clone());
                    proposition_dossier_ids.push(match proposition_nl.dossier_id {
                        None => "".to_string(),
                        Some(dossier_id) => dossier_id.clone(),
                    });
                    proposition_document_ids.push(match proposition_nl.document_id {
                        None => "".to_string(),
                        Some(document_id) => document_id.clone(),
                    });

                    // proposition_id += 1;
                    previous_proposition_nl.clear();
                    previous_proposition_fr.clear();

                    // Download dossier if needed.
                    if has_dossier_id {
                        check_and_download_dossier_file(
                            dossier_id_opt.unwrap().as_str(),
                            session_id,
                            date.parse().unwrap(),
                            client,
                            web_request_count,
                        )
                        .await?;
                    }
                }

                break;
            }
            continue;
        }

        if !processing_propositions {
            continue;
        }

        // Proposition.
        if tag_name == "h2" {
            let french_indicator_words = ["à", "membre"];
            let dutch_indicator_words = ["oproep"];

            // Extract Dutch.
            let dutch_text_raw = element
                .select(&span_selector)
                .filter(|s| matches!(s.value().attr("lang"), Some("NL") | Some("NL-BE")))
                .last()
                .map(|span| span.text().collect::<Vec<_>>().join(" "));

            // Extract French.
            let french_text_raw = element
                .select(&span_selector)
                .filter(|s| s.value().attr("lang") == Some("FR"))
                .last()
                .map(|span| span.text().collect::<Vec<_>>().join(" "));

            let mut cleaned_nl = dutch_text_raw
                .as_ref()
                .map(|t| clean_text(t).replace("\"", "'"));
            let mut cleaned_fr = french_text_raw
                .as_ref()
                .map(|t| clean_text(t).replace("\"", "'"));

            // Check for misclassification
            if let Some(ref text) = cleaned_nl {
                let is_french = french_indicator_words
                    .iter()
                    .any(|&kw| text.to_lowercase().contains(kw));
                if is_french {
                    cleaned_fr = Some(text.clone());
                    cleaned_nl = None;
                }
            }

            if let Some(ref text) = cleaned_fr {
                let is_dutch = dutch_indicator_words
                    .iter()
                    .any(|&kw| text.to_lowercase().contains(kw));
                if is_dutch {
                    cleaned_nl = Some(text.clone());
                    cleaned_fr = None;
                }
            }

            if let Some(cleaned_nl) = cleaned_nl {
                if cleaned_nl.starts_with("-") {
                    previous_proposition_nl.push_str("\n");
                } else if !previous_proposition_nl.is_empty() || !previous_proposition_fr.is_empty()
                {
                    let previous_fr_clone = previous_proposition_fr.clone();
                    let propositions_fr = previous_fr_clone
                        .split('\n')
                        .filter(|s| !s.trim().is_empty())
                        .collect::<Vec<_>>();

                    let previous_nl_clone = previous_proposition_nl.clone();
                    let propositions_nl = previous_nl_clone
                        .split('\n')
                        .filter(|s| !s.trim().is_empty())
                        .collect::<Vec<_>>();

                    for pair in propositions_nl.iter().zip_longest(propositions_fr.iter()) {
                        match pair {
                            EitherOrBoth::Both(text_nl, text_fr) => {
                                let prop_text_nl = text_nl.to_string();
                                let prop_text_fr = text_fr.to_string();

                                // Flush proposition
                                let proposition_nl = extract_proposition_data(
                                    prop_text_nl.clone().replace("- ", ""),
                                )
                                .await?;
                                let proposition_fr = extract_proposition_data(
                                    prop_text_fr.clone().replace("- ", ""),
                                )
                                .await?;
                                let has_dossier_id = proposition_nl.dossier_id.is_some();
                                let dossier_id_opt = proposition_nl.dossier_id.clone();

                                proposition_ids.push(proposition_id.to_string());
                                proposition_meeting_ids.push(meeting_id.to_string());
                                proposition_session_ids.push(session_id.to_string());

                                proposition_titles_nl.push(proposition_nl.topic.clone());
                                proposition_titles_fr.push(proposition_fr.topic.clone());
                                proposition_dossier_ids.push(match proposition_nl.dossier_id {
                                    None => "".to_string(),
                                    Some(id) => id.clone(),
                                });
                                proposition_document_ids.push(match proposition_nl.document_id {
                                    None => "".to_string(),
                                    Some(id) => id.clone(),
                                });

                                proposition_id += 1;

                                // Download dossier if needed.
                                if has_dossier_id {
                                    check_and_download_dossier_file(
                                        dossier_id_opt.unwrap().as_str(),
                                        session_id,
                                        date.parse().unwrap(),
                                        client,
                                        web_request_count,
                                    )
                                    .await?;
                                }

                                previous_proposition_nl.clear();
                                previous_proposition_fr.clear();
                            }
                            _ => {}
                        }
                    }
                }

                previous_proposition_nl.push_str(&cleaned_nl);
            }

            if let Some(cleaned_fr) = cleaned_fr {
                if cleaned_fr.starts_with('-') {
                    previous_proposition_fr.push_str("\n");
                }
                previous_proposition_fr.push_str(&cleaned_fr);
            }
        }
    }

    // Extract votes.
    let mut vote_text_nl = String::new();
    let mut vote_text_fr = String::new();
    let mut previous_vote_title_nl = String::new();
    let mut previous_vote_title_fr = String::new();
    let mut found_votes = false;
    // let mut vote_count: i32 = 0;
    let mut vote_id: i32 = 0;
    let mut collecting_grouped_vote = false;

    // Find vote summaries.
    for element in document.select(&Selector::parse("h1, h2, table").unwrap()) {
        let tag_name = element.value().name();

        // Votes section.
        if tag_name == "h1" {
            let text = element
                .text()
                .collect::<Vec<_>>()
                .join(" ")
                .trim()
                .to_string();
            if text.contains("Naamstemmingen") || text.contains("Naamstemming") {
                found_votes = true;
            }
        }

        if !found_votes {
            continue;
        }

        if tag_name == "h2" {
            let nl_spans: Vec<_> = element
                .select(&span_selector)
                .filter(|s| matches!(s.value().attr("lang"), Some("NL") | Some("NL-BE")))
                .collect();

            let fr_spans: Vec<_> = element
                .select(&span_selector)
                .filter(|s| s.value().attr("lang") == Some("FR"))
                .collect();

            if let Some(last_span) = nl_spans.last() {
                let raw_text = last_span.text().collect::<Vec<_>>().join(" ");
                vote_text_nl = clean_text(&raw_text).replace("\"", "\'");

                // FIXME: What am I actually checking here? That it is not empty?
                if !vote_text_nl.is_empty() && !vote_text_nl.starts_with("-") {
                    /*   if vote_text_nl.starts_with("Moties ingediend")
                    || vote_text_nl.contains("Aangehouden amendement")
                    || vote_text_nl.contains("Geheel van het voorstel van resolutie")
                    || vote_text_nl.contains("Geheel van het wetsvorstel")
                    || vote_text_nl.contains("Wetsontwerp") {*/
                    previous_vote_title_nl = vote_text_nl.clone();
                    collecting_grouped_vote = true;
                } else if collecting_grouped_vote && vote_text_nl.starts_with("-") {
                    previous_vote_title_nl.push_str("\n");
                    previous_vote_title_nl.push_str(&vote_text_nl);
                } else {
                    if !previous_vote_title_nl.is_empty() {
                        // Clear out the grouped vote title, but don’t push votes yet.
                        collecting_grouped_vote = false;
                    }
                    previous_vote_title_nl.clear();
                }
            }

            if let Some(last_span) = fr_spans.last() {
                let raw_text = last_span.text().collect::<Vec<_>>().join(" ");
                vote_text_fr = clean_text(&raw_text).replace("\"", "\'");

                if !vote_text_fr.is_empty() && !vote_text_fr.starts_with("-") {
                    // if vote_text_fr.starts_with("Motions déposées")
                    //     || vote_text_fr.contains("Amendement réservé")
                    //     || vote_text_fr.contains("Ensemble de la proposition de résolution")
                    //     || vote_text_fr.contains("Projet de loi") {

                    previous_vote_title_fr = vote_text_fr.clone();
                    collecting_grouped_vote = true;
                } else if collecting_grouped_vote && vote_text_fr.starts_with("-") {
                    previous_vote_title_fr.push_str("\n");
                    previous_vote_title_fr.push_str(&vote_text_fr);
                } else {
                    if !previous_vote_title_fr.is_empty() {
                        // Clear out the grouped vote title, but don’t push votes yet.
                        collecting_grouped_vote = false;
                    }
                    previous_vote_title_fr.clear();
                }
            }
        }

        if tag_name == "table" {
            let vote = extract_vote(element);
            let (yes_voters, no_voters, abstain_voters) =
                extract_voter_names(&document, &vote.name.replace("Stemming ", ""));

            let converted_yes_voters: Vec<String> = yes_voters
                .split(',')
                .map(|name| convert_name(name.trim()))
                .collect();

            let converted_no_voters: Vec<String> = no_voters
                .split(',')
                .map(|name| convert_name(name.trim()))
                .collect();

            let converted_abstain_voters: Vec<String> = abstain_voters
                .split(',')
                .map(|name| convert_name(name.trim()))
                .collect();

            if vote.yes > 0 || vote.no > 0 || vote.abstain > 0 {
                // if !previous_vote_title_nl.is_empty() || !previous_vote_title_fr.is_empty() {
                //      vote_count += 1;

                vote_ids.push(vote_id.to_string());
                vote_meeting_ids.push(meeting_id.to_string());
                vote_dates.push(date.to_string());
                vote_session_ids.push(session_id.to_string());

                let vote_nl = extract_vote_data(previous_vote_title_nl.clone()).await?;
                let vote_fr = extract_vote_data(previous_vote_title_fr.clone()).await?;

                if !vote_nl.topic.is_empty() {
                    vote_titles_nl.push(vote_nl.topic.clone());
                } else {
                    vote_titles_nl.push(vote_text_nl.clone().to_string());
                }

                if !vote_fr.topic.is_empty() {
                    vote_titles_fr.push(vote_fr.topic.clone());
                } else {
                    vote_titles_fr.push(vote_text_fr.clone().to_string());
                }

                // Download dossier if needed.
                let has_dossier_id = vote_nl.dossier_id.is_some();
                let dossier_id_opt = vote_nl.dossier_id.clone();
                if has_dossier_id {
                    check_and_download_dossier_file(
                        dossier_id_opt.unwrap().as_str(),
                        session_id,
                        date.parse().unwrap(),
                        client,
                        web_request_count,
                    )
                    .await?;
                }

                vote_yes.push(vote.yes.to_string());
                vote_no.push(vote.no.to_string());
                vote_abstain.push(vote.abstain.to_string());
                vote_members_yes.push(converted_yes_voters.join(", "));
                vote_members_no.push(converted_no_voters.join(", "));
                vote_members_abstain.push(converted_abstain_voters.join(", "));
                vote_dossier_ids.push(match vote_nl.dossier_id {
                    None => "".to_string(),
                    Some(dossier_id) => dossier_id.to_string(),
                });
                vote_document_ids.push(match vote_nl.document_id {
                    None => "".to_string(),
                    Some(document_id) => document_id.to_string(),
                });
                vote_motion_ids.push(match vote_nl.motion_id {
                    None => "".to_string(),
                    Some(motion_id) => motion_id.to_string(),
                });

                vote_id += 1;
                //   }
            }
        }
    }

    meeting_session_ids.push(session_id.to_string());
    meeting_ids.push(meeting_id.to_string());
    dates.push(date.to_string());
    times_of_day.push(time_of_day.to_string());
    start_times.push(start_time.to_string());
    end_times.push(end_time.to_string());

    Ok(())
}

async fn check_and_download_dossier_file(
    dossier_id: &str,
    session_id: u32,
    date: NaiveDate,
    client: &ScrapingClient,
    web_request_count: &mut u32,
) -> Result<(), Box<dyn Error>> {
    let dossier_root = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();

    let foldername = format!(
        "data/sources/sessions/{}/dossiers",
        session_id
    );

    let dossier_dir = dossier_root.join(foldername);

    // let dossier_dir = "scraper/data/sources/dossiers";
    let filename_prefix = format!("{}_{}_", session_id, dossier_id);
    let mut should_download = true;

    let mut existing_file: Option<PathBuf> = None;
    let dossier_dir_clone = dossier_dir.clone(); // Clone the original PathBuf

    if let Ok(mut entries) = read_dir(dossier_dir).await {
        while let Ok(Some(entry)) = entries.next_entry().await {
            let fname = entry.file_name().to_string_lossy().to_string();
            if fname.starts_with(&filename_prefix) {
                existing_file = Some(entry.path());

                // Try to extract date
                if let Some(caps) = Regex::new(r"_(\d{4}-\d{2}-\d{2})")?.captures(&fname) {
                    if let Some(date_str) = caps.get(1) {
                        if let Ok(existing_date) =
                            NaiveDate::parse_from_str(date_str.as_str(), "%Y-%m-%d")
                        {
                            should_download = date >= existing_date;
                        }
                    }
                }
                break;
            }
        }
    }

    if should_download {
        if let Some(old_path) = &existing_file {
            let _ = remove_file(old_path).await;
        }

        let today = Local::now().naive_local().date();
        let new_filename = format!(
            "{}/{}_{}_{}.html",
            dossier_dir_clone.display(),
            session_id,
            dossier_id,
            today
        );
        let filepath = Path::new(&new_filename);

        // let today = Local::now().naive_local().date();
        // let new_filename = format!("{}/{}_{}_{}.html", dossier_dir, session_id, dossier_id, today);
        // let filepath = Path::new(&new_filename);

        let url = format!(
            "https://www.dekamer.be/kvvcr/showpage.cfm?section=/flwb&language=nl&cfm=/site/wwwcfm/flwb/flwbn.cfm?lang=N&legislat={}&dossierID={}",
            session_id, dossier_id
        );

        let response = client.get(&url).await?;
        *web_request_count += 1;
        let raw_bytes = response.bytes().await?;
        let (decoded_str, _, _) = WINDOWS_1252.decode(&raw_bytes);
        let content = decoded_str.to_string();
        if let Some(parent) = filepath.parent() {
            create_dir_all(parent).await?;
        }
        write(filepath, content).await?;
    }

    Ok(())
}

struct QuestionData {
    questioners: Vec<String>,
    respondents: Vec<String>,
    topics: Vec<String>,
    discussion: String,
    dossier_ids: Vec<String>,
}

struct PropositionData {
    topic: String,
    dossier_id: Option<String>,
    document_id: Option<String>,
}

struct VoteData {
    topic: String,
    dossier_id: Option<String>,
    document_id: Option<String>,
    motion_id: Option<String>,
}

impl Default for QuestionData {
    fn default() -> Self {
        QuestionData {
            questioners: Vec::new(),
            respondents: Vec::new(),
            topics: Vec::new(),
            discussion: String::new(),
            dossier_ids: Vec::new(),
        }
    }
}

fn get_discussion_json(input: &str) -> String {
    let cleaned_input = clean_text(input);
    // println!("DISC: {}", cleaned_input);

    // Regex to capture timestamps and speakers (including Le président and De voorzitter)
    // FIX: it looks for numbers like 07.01 to find new sections, but it also fell over 20.00 which could just be an hour within the text!
    // SO: new regex which checks that either no characters should be before it
    // (?m)(?:^|(?:NEWPARAGRAPH))[\n\r\s ]*(\d{2}\.\d{2})[\n\r\s ]+([^:]+):|(?:Le  président|De  voorzitter)\s*:
    // instead of
    // (?m)(\d{2}\.\d{2})[\n\r\s ]+([^:]+):|(?:Le  président|De  voorzitter)\s*:
    let speaker_re = Regex::new(
        r"(?m)(?:^|(?:NEWPARAGRAPH))[\n\r\s ]*(\d{2}\.\d{2})[\n\r\s ]+([^:]+):|(?:Le  président|De  voorzitter)\s*:"
    ).unwrap();

    let speaker_name_re = Regex::new(r"^[^(,:\n\r]+").unwrap();
    let title_re = Regex::new(r"^(Minister|De heer|Mevrouw|Le ministre|La ministre|Monsieur|Madame|Eerste minister|Staatssecretaris)\s+").unwrap();

    let mut discussion = Vec::new();
    let mut current_speaker = String::new();
    let mut last_end = 0;

    for cap in speaker_re.captures_iter(&cleaned_input) {
        let match_start = cap.get(0).unwrap().start();
        let text_segment = cleaned_input[last_end..match_start].trim();

        if !current_speaker.is_empty() && !text_segment.is_empty() {
            let clean_text_segment = text_segment
                .replace("Het incident is gesloten.", "")
                .replace("L'incident est clos.", "")
                //.replace("NEWPARAGRAPH", "\n")
                .trim()
                .to_string();

            if !clean_text_segment.is_empty() {
                // println!("SPEAKER: {}", current_speaker);

                discussion.push(json!({
                    "speaker": current_speaker.trim(),
                    "text": clean_text_segment
                }));
            }
        }

        // Determine speaker
        current_speaker = if let Some(full_speaker) = cap.get(2) {
            let speaker_raw = full_speaker.as_str().trim();
            let speaker_stripped = title_re.replace(speaker_raw, "").to_string();
            speaker_name_re
                .captures(&speaker_stripped)
                .and_then(|c| c.get(0))
                .map_or("Onbekend".to_string(), |m| m.as_str().to_string())
        } else {
            // It's the chairperson
            "Voorzitter".to_string()
        };
        // println!("{}", current_speaker);

        last_end = cap.get(0).unwrap().end();
    }

    // Handle last speaker's segment
    if !current_speaker.is_empty() && last_end < cleaned_input.len() {
        let text_segment = &cleaned_input[last_end..];
        let clean_text_segment = text_segment
            .replace("Het incident is gesloten.", "")
            .replace("L'incident est clos.", "")
            .replace("NEWPARAGRAPH", "\n")
            .trim()
            .to_string();

        if !clean_text_segment.is_empty() {
            // println!("{}", current_speaker);
            discussion.push(json!({
                "speaker": current_speaker.trim(),
                "text": clean_text_segment
            }));
        }
    }

    serde_json::to_string_pretty(&discussion).unwrap()
}

async fn extract_proposition_data(
    proposition_text: String,
) -> Result<PropositionData, Box<dyn Error>> {
    let regex_1 = Regex::new(
        r#"^((?:Voorstel van resolutie|Proposition de résolution|Wetsvoorstel|Proposition de loi|Wetsontwerp|Projet de loi|Voorstel tot|Proposition visant).*)\((\d+)\/(\d+(?:-\d+)?)\)$"#,
    )?;
    let regex_2 = Regex::new(r#"^([^(]*)"#)?;

    if let Some(captures) = regex_1.captures(&proposition_text) {
        let topic = captures
            .get(1)
            .ok_or("Missing topic capture")?
            .as_str()
            .trim()
            .to_string();
        let dossier_id = captures
            .get(2)
            .ok_or("Missing dossier_id capture")?
            .as_str()
            .trim()
            .to_string();
        let document_id = captures
            .get(3)
            .ok_or("Missing document_id capture")?
            .as_str()
            .trim()
            .to_string();

        Ok(PropositionData {
            topic,
            dossier_id: Some(dossier_id),
            document_id: Some(document_id),
        })
    } else if let Some(captures) = regex_2.captures(&proposition_text) {
        // Could not find any ids, just a topic.
        let topic = captures
            .get(1)
            .ok_or("Missing topic capture")?
            .as_str()
            .trim()
            .to_string();

        Ok(PropositionData {
            topic,
            dossier_id: None,
            document_id: None,
        })
    } else {
        Err("No regex matched for proposition text".into())
    }
}

async fn extract_vote_data(vote_text: String) -> Result<VoteData, Box<dyn Error>> {
    let regex_1 = Regex::new(r#"^(.*)\((\d+)/(\d+(?:-\d+)?)\)\s*$"#)?; // with (123/1-2)
    let regex_2 = Regex::new(r#"^(.*)\s+\((?:nr\.|n°)\s*(\d+)\)\s*$"#)?; // with (nr. 12)
    let regex_3 = Regex::new(r#"^([^(]*)"#)?; // without any number

    if let Some(capture) = regex_1.captures(&vote_text) {
        let topic = capture
            .get(1)
            .ok_or("Missing topic capture")?
            .as_str()
            .trim()
            .to_string();
        let dossier_id = capture
            .get(2)
            .ok_or("Missing dossier_id capture")?
            .as_str()
            .trim()
            .to_string();
        let document_id = capture
            .get(3)
            .ok_or("Missing document_id capture")?
            .as_str()
            .trim()
            .to_string();

        Ok(VoteData {
            topic,
            dossier_id: Some(dossier_id),
            document_id: Some(document_id),
            motion_id: None,
        })
    } else if let Some(capture) = regex_2.captures(&vote_text) {
        let topic = capture
            .get(1)
            .ok_or("Missing topic capture")?
            .as_str()
            .trim()
            .to_string();
        let motion_id = capture
            .get(2)
            .ok_or("Missing motion_id capture")?
            .as_str()
            .trim()
            .to_string();

        Ok(VoteData {
            topic,
            dossier_id: None,
            document_id: None,
            motion_id: Some(motion_id),
        })
    } else if let Some(capture) = regex_3.captures(&vote_text) {
        let topic = capture
            .get(1)
            .ok_or("Missing topic capture")?
            .as_str()
            .trim()
            .to_string();

        Ok(VoteData {
            topic,
            dossier_id: None,
            document_id: None,
            motion_id: None,
        })
    } else {
        Err("No regex matched for vote text".into())
    }
}


async fn extract_question_data(
    typo_map: HashMap<String, String>,
    question_text: String,
    discussion_text: String,
) -> Result<QuestionData, Box<dyn Error>> {
    // Below are some of the issues found in meeting reports.
    // ISSUE: plenary meeting report 071: questions did not have dossier ids mentioned. -> Adjusted regex to not require this
    // let regex = Regex::new(
    //     r#"(?m)(?:(?:Vraag van|Question de)\s)?([^\n]+?)\s+(?:aan|à)\s+([^\n]+?)\s*\(.*?\)\s*(?:over|sur)\s*["“'](.+?)["”'](?:\s*\((\d{8}[A-Z])\))?"#,
    // )?;
    // ISSUE: plenary meeting report 073: question title with single tick in it (such as OCMW's) broke the regex -> Adjusted regex
    // let regex = Regex::new(
    //     r#"(?m)(?:(?:Vraag van|Question de)\s)?([^\n]+?)\s+(?:aan|à)\s+([^\n]+?)\s*\(.*?\)\s*(?:over|sur)\s*[“"]([^“"]+)[”"](?:\s*\((\d{8}[A-Z])\))?"#
    // )?;
    let regex = Regex::new(r#"(?m)(?:(?:Vraag van|Question de)\s)?([^\n]+?)\s+(?:aan|à)\s+([^\n]+?)\s*\(.*?\)\s*(?:over|sur)\s*(?:"([^"]+?)"|“([^”]+?)”|'([^']+?)')(?:\s*\((\d{8}[A-Z])\))?"#)?;



    let mut questioners = Vec::new();
    let mut topics = Vec::new();
    let mut respondents = Vec::new();
    let mut dossier_ids = Vec::new();

    for capture in regex.captures_iter(&question_text) {
        let questioner_raw = capture
            .get(1)
            .unwrap()
            .as_str()
            .trim()
            .replace("- ", "")
            .to_string();
        let questioner = typo_map
            .get(&questioner_raw)
            .cloned()
            .unwrap_or(questioner_raw);

        let respondent = capture.get(2).unwrap().as_str().trim().to_string();

        // topic may be in group 3, 4 or 5 depending on which branch matched
        let topic = capture
            .get(3)
            .or_else(|| capture.get(4))
            .or_else(|| capture.get(5))
            .map(|m| m.as_str().trim().to_string())
            .unwrap_or_else(|| "".to_string());

        let dossier_id = match capture.get(6) {
            Some(m) => format!("Q{}", m.as_str().trim()),
            None => "".to_string(),
        };

        questioners.push(questioner);
        if !respondents.contains(&respondent) {
            respondents.push(respondent);
        }
        dossier_ids.push(dossier_id);
        topics.push(topic);

        // let questioner_raw = capture
        //     .get(1)
        //     .unwrap()
        //     .as_str()
        //     .trim()
        //     .replace("- ", "")
        //     .to_string();
        // let questioner = typo_map // apply tpo fixes
        //     .get(&questioner_raw)
        //     .cloned()
        //     .unwrap_or(questioner_raw);
        // let respondent = capture.get(2).unwrap().as_str().trim().to_string();
        // let topic = capture.get(3).unwrap().as_str().trim().to_string();
        // let dossier_id = match capture.get(4) {
        //     Some(m) => format!("Q{}", m.as_str().trim()),
        //     None => "".to_string(),
        // };
        // questioners.push(questioner);
        // if !respondents.contains(&respondent) {
        //     respondents.push(respondent);
        // }
        // dossier_ids.push(dossier_id);
        // topics.push(topic);
    }

    Ok(QuestionData {
        questioners,
        respondents,
        topics,
        discussion: get_discussion_json(&discussion_text),
        dossier_ids,
    })
}

fn convert_name(name: &str) -> String {
    let parts: Vec<&str> = name.split_whitespace().collect();

    if parts.is_empty() {
        return "".to_string();
    }

    // If there's only one part, return as is (just a single name)
    if parts.len() == 1 {
        return name.to_string();
    }

    // Last name is everything except the last word (first name)
    let first_name = parts.last().unwrap();
    let last_name = parts[..parts.len() - 1].join(" ");

    // Combine them in "First Name Last Name" format
    format!("{} {}", first_name, last_name)
}

struct VoteRecord {
    name: String,
    yes: u32,
    no: u32,
    abstain: u32,
}

fn extract_vote(table: ElementRef) -> VoteRecord {
    let row_selector = Selector::parse("tr").unwrap();
    let cell_selector = Selector::parse("td").unwrap();
    let rows = table.select(&row_selector);

    let mut name = String::new();
    let mut processing_rows = false;

    let mut yes = 0;
    let mut no = 0;
    let mut abstain = 0;

    for (i, row) in rows.enumerate() {
        let cells: Vec<_> = row.select(&cell_selector).collect();

        // First row contains the description (title)
        if i == 0 && !cells.is_empty() {
            let text = row.text().collect::<Vec<_>>().join(" ").trim().to_string();
            if text.contains("Stemming/vote") {
                name = text.replace("(", "").replace(")", "").replace("/vote", "");
                processing_rows = true;
            }
            continue; // Move to the next row
        }

        if processing_rows && cells.len() >= 2 {
            let label = cells[0]
                .text()
                .collect::<Vec<_>>()
                .join(" ")
                .trim()
                .to_string();
            let value_text: String = cells[1].text().collect::<Vec<_>>().join(" "); // Store in a String
            let value_trimmed = value_text.trim(); // Now trim it safely

            if let Ok(value) = value_trimmed.parse::<u32>() {
                match label.as_str() {
                    "Ja" => yes = value,
                    "Nee" => no = value,
                    "Onthoudingen" => abstain = value,
                    _ => {}
                }
            }

            if let Ok(value) = value_text.parse::<u32>() {
                match label.as_str() {
                    "Ja" => yes = value,
                    "Nee" => no = value,
                    "Onthoudingen" => abstain = value,
                    _ => {}
                }
            }
        }
    }

    VoteRecord {
        name,
        yes,
        no,
        abstain,
    }
}

fn extract_voter_names(document: &Html, vote_index: &str) -> (String, String, String) {
    let mut yes_voters = String::new();
    let mut no_voters = String::new();
    let mut abstain_voters = String::new();

    let span_selector  = Selector::parse("span").unwrap();
    let td_selector    = Selector::parse("td").unwrap();
    let p_selector     = Selector::parse("p").unwrap();

    // 1. find the three result‑tables that belong to the requested vote
    let mut tables = Vec::new();
    for span in document.select(&span_selector) {
        if span.text().collect::<Vec<_>>().join(" ")
            .contains(&format!("Vote nominatif - Naamstemming: {}", vote_index)) || span.text().collect::<Vec<_>>().join(" ")
            .contains(&format!("Naamstemming - Vote nominatif: {}", vote_index))
        {
            let mut node = span.parent();
            while let Some(n) = node {
                if let Some(el) = ElementRef::wrap(n) {
                    if el.value().name() == "table" {
                        tables.push(el);
                        if tables.len() == 3 { break; }
                    }
                }
                node = n.next_sibling();
            }
            break;
        }
    }

    //------------------------------------------------------------------
    // 2. for each of the three tables:                  YES / NO / ABS
    //------------------------------------------------------------------
    let vote_types = [&mut yes_voters, &mut no_voters, &mut abstain_voters];
    for (i, table) in tables.iter().enumerate() {
        //--------------------------------------------------------------
        // 2a. read the number in the second <td>; if it is “0” there
        //     are no names and we can skip the expensive scan.
        //--------------------------------------------------------------
        let mut tds = table.select(&td_selector);
        tds.next();                                           // label
        let count: usize = tds
            .next()
            .map(|td| td.text().collect::<Vec<_>>().join(" ").trim().parse().unwrap_or(0))
            .unwrap_or(0);

        if count == 0 {
            *vote_types[i] = String::new();
            continue;                 // go to the next of the 3 tables
        }

        //--------------------------------------------------------------
        // 2b. otherwise: walk forward until the next <table>, pick the
        //     FIRST <p><span> that looks like a name list.
        //--------------------------------------------------------------
        let mut node = table.next_sibling();
        while let Some(n) = node {
            if let Some(el) = ElementRef::wrap(n) {
                // stop when we reach the next result‑table
                if el.value().name() == "table" { break; }

                if el.value().name() == "p" {
                    // first child should be the <span> that holds the text
                    if let Some(span_node) = el.first_child() {
                        if let Some(span_el) = ElementRef::wrap(span_node) {
                            if span_el.value().name() == "span" {
                                let raw = span_el.text().collect::<Vec<_>>().join(" ").trim().to_string();

                                // ───────────  good‑enough heuristic  ───────────
                                // • starts with a letter (not the digit of “3 Vote…”)
                                // • does not contain “Vote nominatif”
                                // • has at least one alphabetic letter
                                //   (filters out empty “&nbsp;” paragraphs)
                                // This accepts one name (“Peeters Karel”)
                                // and many comma‑separated names alike.
                                // ────────────────────────────────────────────────
                                let looks_like_names =
                                    raw.chars().next().map(|c| c.is_alphabetic()).unwrap_or(false)
                                        && !raw.contains("Vote nominatif")
                                        && raw.chars().any(|c| c.is_alphabetic());

                                if looks_like_names {
                                    *vote_types[i] = raw
                                        .replace(", ", ",")
                                        .replace(",\n", ",")
                                        .replace('\n', " ")
                                        .to_string();
                                    break;      // done with this vote‑type
                                }
                            }
                        }
                    }
                }
            }
            node = n.next_sibling();
        }
    }

    (yes_voters, no_voters, abstain_voters)
}


// fn extract_voter_names(document: &Html, vote_index: &str) -> (String, String, String) {
//     let mut yes_voters = String::new();
//     let mut no_voters = String::new();
//     let mut abstain_voters = String::new();
//
//     let span_selector = Selector::parse("span").unwrap();
//     let mut tables = vec![];
//
//     // Step 1: Locate the correct vote section
//     for span in document.select(&span_selector) {
//         let text = span.text().collect::<Vec<_>>().join(" ");
//         if text.contains(&format!("Vote nominatif - Naamstemming: {}", vote_index)) {
//             tables.clear();
//
//             let mut next_node = span.parent();
//             while let Some(node) = next_node {
//                 if let Some(el) = ElementRef::wrap(node) {
//                     if el.value().name() == "table" {
//                         tables.push(el);
//                         if tables.len() == 3 {
//                             break;
//                         }
//                     }
//                 }
//                 next_node = node.next_sibling();
//             }
//             break;
//         }
//     }
//
//     let vote_types = [&mut yes_voters, &mut no_voters, &mut abstain_voters];
//
//     for (i, table) in tables.iter().enumerate() {
//         let mut this_table_has_no_votes = true;
//         let mut next_node = table.next_sibling();
//         while let Some(node) = next_node {
//             if let Some(el) = ElementRef::wrap(node) {
//                 if el.value().name() == "table" {
//                     break; // Stop processing if another table is found
//                 }
//
//                 // Check if the node is a <p> element, then look for the <span> inside it
//                 if el.value().name() == "p" {
//                     // Try to find the <span> element within the <p>
//                     if let Some(span_node) = el.first_child() {
//                         if let Some(span_el) = ElementRef::wrap(span_node) {
//                             if span_el.value().name() == "span" {
//                                 let names = span_el
//                                     .text()
//                                     .collect::<Vec<_>>()
//                                     .join(" ")
//                                     .trim()
//                                     .to_string();
//
//                                 let looks_like_names = names.contains(',')
//                                     && !names.chars().any(|c| c.is_ascii_digit()); // no vote index digit
//
//
//
//                                 if looks_like_names {
//                                     this_table_has_no_votes = false;
//                                     *vote_types[i] = names
//                                         .replace(", ", ",")
//                                         .replace(",\n", ",")
//                                         .replace("\n", " ")
//                                         .trim()
//                                         .to_string();
//                                     break;
//                                 }
//
//
//                                 //
//                                 // if names.contains(",") {
//                                 //     // Ensure it's a valid name list
//                                 //     this_table_has_no_votes = false;
//                                 //     *vote_types[i] = names
//                                 //         .replace(", ", ",")
//                                 //         .replace(",\n", ",")
//                                 //         .replace("\n", " ")
//                                 //         .trim()
//                                 //         .to_string();
//                                 //     break; // Stop searching once we find the first valid names list
//                                 // }
//                             }
//                         }
//                     }
//                 }
//             }
//             next_node = node.next_sibling();
//         }
//         if this_table_has_no_votes {
//             *vote_types[i] = String::new();
//         }
//     }
//
//     (yes_voters, no_voters, abstain_voters)
// }

fn extract_time_from_document(document: &Html, keyword: &str) -> Result<String, Box<dyn Error>> {
    let span_selector = Selector::parse("span").unwrap();
    let re = Regex::new(r"(\d{1,2})\.(\d{2})\s*0?uur").unwrap();

    let mut last_time: Option<String> = None;

    // Iterate through all spans.
    for span in document.select(&span_selector) {
        let text = span.text().collect::<Vec<_>>().join(" ");
        // println!("KEYWORD: {}", keyword);
        //println!("TEXT: {}", text);
        if text
            .to_lowercase()
            .contains(keyword.to_lowercase().as_str())
        {
            // println!("TEXT: {}", text);
            // Try to capture the time pattern.
            if let Some(caps) = re.captures(&text) {
                // Update last_time with the most recent time match.
                last_time = Some(format!("{}h{}", &caps[1], &caps[2]));
            }
        }
    }

    // Return the last valid time found, or an error if no time was found.
    match last_time {
        Some(time) => Ok(time),
        None => Err("Could not extract time from the document".into()),
    }
}

fn extract_start_time_from_document(document: &Html) -> Result<String, Box<dyn Error>> {
    let phrases = vec![
        "De vergadering wordt geopend",
        "De vergadering wordt hervat",
    ];

    for phrase in phrases {
        if let Ok(time) = extract_time_from_document(document, phrase) {
            return Ok(time);
        }
    }

    Err("Could not extract start time from the document".into())
}

fn extract_end_time_from_document(document: &Html) -> Result<String, Box<dyn Error>> {
    let phrases = vec![
        "De vergadering wordt gesloten",
        "De vergadering wordt geschorst",
    ];

    for phrase in phrases {
        if let Ok(time) = extract_time_from_document(document, phrase) {
            return Ok(time);
        }
    }

    Err("Could not extract end time from the document".into())
}

fn extract_time_of_day_from_document(document: &Html) -> Result<String, Box<dyn Error>> {
    let span_selector = Selector::parse("span").unwrap();
    for span in document.select(&span_selector) {
        let text = span
            .text()
            .collect::<Vec<_>>()
            .join(" ")
            .trim()
            .to_lowercase();

        if text == "namiddag" {
            return Ok("afternoon".to_string());
        } else if text == "voormiddag" {
            return Ok("morning".to_string());
        } else if text == "avond" {
            return Ok("evening".to_string());
        }
    }
    Err("Could not extract time of day from the document".into())
}

fn extract_date_from_document(document: &Html) -> Result<String, Box<dyn Error>> {
    let table_selector = Selector::parse("table").unwrap();
    if let Some(first_table) = document.select(&table_selector).next() {
        let span_selector = Selector::parse("span").unwrap();
        let mut extracted_text = String::new();

        for span in first_table.select(&span_selector) {
            let text = span.text().collect::<Vec<_>>().join(" ");
            extracted_text.push_str(&text);
            extracted_text.push(' ');
        }

        let re = Regex::new(r"(\d{1,2})\s+([a-zA-Z]+)\s+(\d{4})").unwrap();
        if let Some(caps) = re.captures(&extracted_text) {
            let day = format!("{:02}", caps[1].parse::<u8>()?);
            let month = match &caps[2].to_lowercase()[..] {
                "januari" => "01",
                "februari" => "02",
                "maart" => "03",
                "april" => "04",
                "mei" => "05",
                "juni" => "06",
                "juli" => "07",
                "augustus" => "08",
                "september" => "09",
                "oktober" => "10",
                "november" => "11",
                "december" => "12",
                _ => return Err("Invalid month name".into()),
            };
            let year = &caps[3];
            return Ok(format!("{}-{}-{}", year, month, day));
        }
    }
    Err("Could not extract date from the document".into())
}
