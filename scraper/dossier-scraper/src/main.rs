use arrow::array::{ArrayRef, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use chrono::Local;
use crawl::client::ScrapingClient;
use encoding_rs::WINDOWS_1252;
use indicatif::{ProgressBar, ProgressStyle};
use parquet::arrow::ArrowWriter;
use scraper::{Html, Selector};
use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::fs::{self, File, read_to_string};
use std::path::Path;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Create client.
    let client = ScrapingClient::new();
    let session_id = 56;

    // Create folders.
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("web/src/data");

    let dossiers_path = root.join("dossiers.parquet");
    let subdocuments_path = root.join("subdocuments.parquet");

    if let Some(parent) = dossiers_path.parent() {
        fs::create_dir_all(parent)?;
    }
    if let Some(parent) = subdocuments_path.parent() {
        fs::create_dir_all(parent)?;
    }

    // Create parquet files.
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

    let subdocuments_file = File::create(subdocuments_path).unwrap();
    let subdocuments_schema = Arc::new(Schema::new(vec![
        Field::new("dossier_id", DataType::Utf8, false),
        Field::new("id", DataType::Utf8, false),
        Field::new("date", DataType::Utf8, false),
        Field::new("type", DataType::Utf8, false),
        Field::new("authors", DataType::Utf8, false),
    ]));

    // Vectors to hold data
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

    let mut dossier_id_counter = 1;
    let mut consecutive_failures = 0;
    let max_consecutive_failures = 50;

    let pb = ProgressBar::new_spinner();
    pb.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.green} [{elapsed_precise}] Dossier {pos} (Failures: {msg})")?,
    );

    let source_root = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
    let dossiers_dir = source_root.join(format!("data/sources/sessions/{}/dossiers", session_id));
    
    // Pre-scan existing files
    let mut existing_dossiers = HashMap::new();
    if dossiers_dir.exists() {
        for entry in fs::read_dir(&dossiers_dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_file() {
                if let Some(filename) = path.file_name().and_then(|s| s.to_str()) {
                    // Format: 56_34_2025-12-16.html
                    if filename.starts_with(&format!("{}_", session_id)) && filename.ends_with(".html") {
                        let parts: Vec<&str> = filename.split('_').collect();
                        if parts.len() >= 3 {
                            // parts[0] = session, parts[1] = dossier_id, parts[2] = date.html
                            let dossier_id = parts[1].to_string();
                            existing_dossiers.insert(dossier_id, path);
                        }
                    }
                }
            }
        }
    }

    // PDF Download directory
    let pdf_download_dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("pdf-parser/downloads");
    
    if !pdf_download_dir.exists() {
        fs::create_dir_all(&pdf_download_dir)?;
    }

    loop {
        if consecutive_failures >= max_consecutive_failures {
            pb.finish_with_message(format!(
                "Stopped after {} consecutive failures at dossier {}",
                consecutive_failures, dossier_id_counter
            ));
            break;
        }

        let dossier_id_str = format!("{}", dossier_id_counter);
        
        // Determine file path
        let filepath = if let Some(path) = existing_dossiers.get(&dossier_id_str) {
            path.clone()
        } else {
            let current_date = Local::now().format("%Y-%m-%d").to_string();
            let filename = format!("{}_{}_{}.html", session_id, dossier_id_str, current_date);
            dossiers_dir.join(filename)
        };

        let content = if filepath.exists() {
             read_to_string(&filepath)?
        } else {
            let url = format!(
                "https://www.dekamer.be/kvvcr/showpage.cfm?section=/flwb&language=nl&cfm=/site/wwwcfm/flwb/flwbn.cfm?lang=N&legislat={}&dossierID={}",
                session_id, dossier_id_counter
            );

            let response = client.get(&url).await?;
            let raw_bytes = response.bytes().await?;
            let (decoded_str, _, _) = WINDOWS_1252.decode(&raw_bytes);
            let content = decoded_str.to_string();

            if let Some(parent) = filepath.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::write(&filepath, &content).unwrap();
            content
        };

        // Check for soft 404

        // Check for soft 404
        // User provided example: "dossier = 56K1300 not found" inside <div id="Story">
        if content.contains("not found") && content.contains("dossier =") {
             consecutive_failures += 1;
             pb.set_message(format!("{}", consecutive_failures));
             pb.set_position(dossier_id_counter);
             dossier_id_counter += 1;
             continue;
        }

        // Parse content
        let document = Html::parse_document(&content);
        
        // Sometimes the page returns but might be empty or valid. 
        // If we successfully parse a title, we assume it's valid.
        // If scrape_dossier returns a mostly empty dossier, we might need to check that.
        // But let's assume if it's not the "not found" page, it's valid or scrape_dossier handles it.

        match scrape_dossier(&dossier_id_str, &document).await {
            Ok(dossier) => {
                // If title is empty, it might be a weird page, but let's count it as valid if not explicitly 404?
                // Or maybe treat empty title as failure?
                if dossier.title.is_empty() {
                     // Could be an edge case. For now, treat as failure to be safe? 
                     // Or just accept it. The scraping logic in plenary-scraper allowed empty titles (unwrap_or("")).
                     // Let's count it as success but maybe log it?
                     // Actually, if title is empty, it's likely not a real dossier page or structure changed.
                     // But let's stick to the explicit "not found" text check for the counter.
                }

                dossier_session_ids.push(session_id.to_string());
                dossier_ids.push(dossier_id_str.clone());
                dossier_titles.push(dossier.title);
                dossier_authors.push(dossier.authors.join(",").to_string());
                dossier_submission_dates.push(dossier.submission_date);
                dossier_end_dates.push(dossier.end_date);
                dossier_vote_dates.push(dossier.vote_date);
                dossier_document_types.push(dossier.document_type.to_string());
                dossier_statuses.push(dossier.status.to_string());

                for subdocument in &dossier.subdocuments {
                    subdocument_dossier_ids.push(subdocument.dossier_id.clone());
                    subdocument_ids.push(subdocument.id.clone());
                    subdocument_dates.push(subdocument.date.clone());
                    subdocument_types.push(subdocument.document_type.to_string());
                    subdocument_authors.push(subdocument.authors.join(",").to_string());

                    // Download PDF
                    let dossier_id_padded = format!("{:0>4}", dossier_id_str);
                    let filename = format!("{}K{}{}.pdf", session_id, dossier_id_padded, subdocument.id);
                    let url = format!(
                        "https://www.dekamer.be/FLWB/PDF/{}/{}/{}",
                        session_id, dossier_id_padded, filename
                    );

                    let dossier_pdf_dir = pdf_download_dir.join(&dossier_id_str);
                    if !dossier_pdf_dir.exists() {
                         let _ = fs::create_dir_all(&dossier_pdf_dir);
                    }
                    
                    let pdf_path = dossier_pdf_dir.join(&filename);

                    if !pdf_path.exists() {
                        // pb.println(format!("Downloading {}", url)); // Optional: Log download
                        match client.get(&url).await {
                            Ok(response) => {
                                if response.status().is_success() {
                                    if let Ok(bytes) = response.bytes().await {
                                        // Simple validation: Check if it starts with %PDF
                                        if bytes.starts_with(b"%PDF") {
                                            if let Err(e) = fs::write(&pdf_path, bytes) {
                                                 eprintln!("Failed to write PDF {}: {}", filename, e);
                                            }
                                        } else {
                                            // pb.println(format!("Invalid PDF (not %PDF) for {}", url));
                                        }
                                    }
                                }
                            },
                            Err(e) => eprintln!("Failed to download {}: {}", url, e),
                        }
                        // Polite delay
                        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    }
                }

                consecutive_failures = 0; // Reset counter on success
            }
            Err(e) => {
                eprintln!("Error scraping dossier {}: {}", dossier_id_counter, e);
                // Treat as failure?
                consecutive_failures += 1;
            }
        }

        pb.set_message(format!("{}", consecutive_failures));
        pb.set_position(dossier_id_counter);
        dossier_id_counter += 1;
    }

    // Write Parquet files
    let dossiers_batch = RecordBatch::try_new(
        dossiers_schema.clone(),
        vec![
            Arc::new(StringArray::from(dossier_session_ids)) as ArrayRef,
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

    let subdocuments_batch = RecordBatch::try_new(
        subdocuments_schema.clone(),
        vec![
            Arc::new(StringArray::from(subdocument_dossier_ids)) as ArrayRef,
            Arc::new(StringArray::from(subdocument_ids)),
            Arc::new(StringArray::from(subdocument_dates)),
            Arc::new(StringArray::from(subdocument_types)),
            Arc::new(StringArray::from(subdocument_authors)),
        ],
    )?;
    let mut subdocuments = ArrowWriter::try_new(subdocuments_file, subdocuments_schema, None)?;
    subdocuments.write(&subdocuments_batch)?;
    subdocuments.close().unwrap();

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
    TabellenOfLijsten,
    Beleidsnota,
    ArtikelenAangenomenInPlenum,
    Kaft,
    Regeerakkoord,
    Corrigendum,
    Bijlage,
    VoorstelVanVerklaring,
    BeslissingOverlegcommissie,
    Begroting,
    VoordrachtVanKandidaten,
    LijstVanVerzoekschriften,
    Beleidsverklaring,
    Verantwoording,
    NietGeevoceerdOntwerp,
    Errata,
    OpmerkingenVanHetRekenhof,
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
    } else if raw.contains("verslag") {
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
    } else if raw.contains("tabellen of lijsten") {
        DocumentType::TabellenOfLijsten
    } else if raw.contains("beleidsnota") {
        DocumentType::Beleidsnota
    } else if raw.contains("artikelen aangenomen in plenum") {
        DocumentType::ArtikelenAangenomenInPlenum
    } else if raw.contains("kaft") {
        DocumentType::Kaft
    } else if raw.contains("regeerakkoord") {
        DocumentType::Regeerakkoord
    } else if raw.contains("corrigendum") {
        DocumentType::Corrigendum
    } else if raw.contains("bijlage") {
        DocumentType::Bijlage
    } else if raw.contains("voorstel van verklaring") {
        DocumentType::VoorstelVanVerklaring
    } else if raw.contains("beslissing overlegcommissie") {
        DocumentType::BeslissingOverlegcommissie
    } else if raw.contains("begroting") {
        DocumentType::Begroting
    } else if raw.contains("voordracht van kandidaten") {
        DocumentType::VoordrachtVanKandidaten
    } else if raw.contains("lijst van verzoekschriften") {
        DocumentType::LijstVanVerzoekschriften
    } else if raw.contains("beleidsverklaring") {
        DocumentType::Beleidsverklaring
    } else if raw.contains("verantwoording") {
        DocumentType::Verantwoording
    } else if raw.contains("niet-geevoceerd ontwerp") {
        DocumentType::NietGeevoceerdOntwerp
    } else if raw.contains("errata") {
        DocumentType::Errata
    } else if raw.contains("opmerkingen van het rekenhof") {
        DocumentType::OpmerkingenVanHetRekenhof
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
    let mut distribution_date = String::new();
    let mut main_document_id = String::new();
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
                    let label_text = cell_1.text().collect::<String>();
                    let label = label_text
                        .split_whitespace()
                        .collect::<Vec<_>>()
                        .join(" ")
                        .to_lowercase();

                    let value_text = cell_2.text().collect::<String>();
                    let value = value_text
                        .split_whitespace()
                        .collect::<Vec<_>>()
                        .join(" ")
                        .to_lowercase();

                    // Main document ID.
                    if label.contains("document kamer") {
                        if let Some(link) = cell_2.select(&Selector::parse("a").unwrap()).last() {
                            if let Some(text) = link.text().next() {
                                main_document_id = text.trim().to_string();
                            }
                        }
                    }
                    // Distribution date.
                    else if label.contains("datum ronddeling") {
                        distribution_date = value.clone();
                    }
                    // Submission date.
                    else if label.contains("indieningsdatum") {
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
                                let label_text = cell_1.text().collect::<String>();
                                let label = label_text
                                    .split_whitespace()
                                    .collect::<Vec<_>>()
                                    .join(" ")
                                    .to_lowercase();

                                let value_text = cell_2.text().collect::<String>();
                                let value = value_text
                                    .split_whitespace()
                                    .collect::<Vec<_>>()
                                    .join(" ")
                                    .to_lowercase();

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

    // Add main document if found.
    if !main_document_id.is_empty() {
        let short_id = if main_document_id.len() >= 3 {
             let suffix = &main_document_id[main_document_id.len()-3..];
             if suffix.chars().all(|c| c.is_numeric()) {
                 suffix.to_string()
             } else {
                 main_document_id.clone()
             }
        } else {
             main_document_id.clone()
        };

        // Check if not already in subdocuments (avoid duplicates).
        if !subdocuments.iter().any(|d| d.id == short_id) {
            subdocuments.insert(0, Subdocument {
                dossier_id: dossier_id.to_string(),
                id: short_id,
                document_type, 
                date: if !distribution_date.is_empty() { distribution_date.clone() } else { submission_date.clone() },
                authors: dossier_authors.clone(),
            });
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

