use arrow::array::{ArrayRef, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use crawl::client::ScrapingClient;
use crawl::utils::clean_text;
use encoding_rs::WINDOWS_1252;
use http::StatusCode;
use parquet::arrow::ArrowWriter;
use regex::Regex;
use scraper::{Html, Selector};
use serde_json::json;
use std::error::Error;
use std::fmt;
use std::fs::{File, read_to_string};
use std::path::Path;
use std::sync::Arc;
use tokio::fs;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Create scraping client.
    let client = ScrapingClient::new();

    let session_id = 56;

    // Create folders.
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("../web/src/data");

    let commissions_path = root.join("commissions.parquet");
    let questions_path = root.join("commission_questions.parquet");

    if let Some(parent) = commissions_path.parent() {
        fs::create_dir_all(parent).await?;
    }
    if let Some(parent) = questions_path.parent() {
        fs::create_dir_all(parent).await?;
    }

    // Create parquet files.
    let commissions_file = File::create(commissions_path).unwrap();
    let commissions_schema = Arc::new(Schema::new(vec![
        Field::new("session_id", DataType::Utf8, false),
        Field::new("commission_id", DataType::Utf8, false),
        Field::new("date", DataType::Utf8, false),
        Field::new("time_of_day", DataType::Utf8, false),
        Field::new("start_time", DataType::Utf8, false),
        Field::new("end_time", DataType::Utf8, false),
        Field::new("commission", DataType::Utf8, false),
        Field::new("chair", DataType::Utf8, false),
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

    // Scrape data.
    let mut commission_ids = vec![];
    let mut commission_session_ids = vec![];
    let mut dates = vec![];
    let mut times_of_day = vec![];
    let mut start_times = vec![];
    let mut end_times = vec![];
    let mut commissions = vec![];
    let mut chairs = vec![];
    let mut question_ids = vec![];
    let mut question_session_ids = vec![];
    let mut question_meeting_ids = vec![];
    let mut question_questioners = vec![];
    let mut question_respondents = vec![];
    let mut question_topics_nl = vec![];
    let mut question_topics_fr = vec![];
    let mut question_discussions = vec![];
    let mut question_dossier_ids = vec![];

    let mut web_request_count = 0;

    // Check for new commission meeting.
    let commission_id_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("current_commission_id.txt");
    let current_commission_id: u32 = std::fs::read_to_string(&commission_id_path)?
        .trim()
        .parse()?;

    let mut last_commission_id = current_commission_id;

    loop {
        let probe = last_commission_id + 1;
        let url = format!(
            "https://www.dekamer.be/doc/CCRI/html/{}/ic{:03}x.html",
            session_id, probe
        );
        let resp = client.get(&url).await?;
        web_request_count += 1;

        if resp.status() == StatusCode::NOT_FOUND {
            break;
        } else {
            // found a new one, move forward
            last_commission_id = probe;
        }
    }

    if last_commission_id == current_commission_id {
        println!("No new meeting available.");
    } else {
        println!("Found new meetings up to {}", last_commission_id);
    }

    for commission in 1..=last_commission_id {
        // println!("SCRAPING: {}", commission);
        scrape_commission(
            &client,
            56,
            commission,
            &mut commission_ids,
            &mut commission_session_ids,
            &mut dates,
            &mut times_of_day,
            &mut start_times,
            &mut end_times,
            &mut commissions,
            &mut chairs,
            &mut question_ids,
            &mut question_session_ids,
            &mut question_meeting_ids,
            &mut question_questioners,
            &mut question_respondents,
            &mut question_discussions,
            &mut question_topics_nl,
            &mut question_topics_fr,
            &mut question_dossier_ids,
            &mut web_request_count,
        )
        .await?;
    }

    std::fs::write(&commission_id_path, last_commission_id.to_string())?;

    let commissions_batch = RecordBatch::try_new(
        commissions_schema.clone(),
        vec![
            Arc::new(StringArray::from(commission_session_ids)) as ArrayRef,
            Arc::new(StringArray::from(commission_ids)),
            Arc::new(StringArray::from(dates)),
            Arc::new(StringArray::from(times_of_day)),
            Arc::new(StringArray::from(start_times)),
            Arc::new(StringArray::from(end_times)),
            Arc::new(StringArray::from(commissions)),
            Arc::new(StringArray::from(chairs)),
        ],
    )?;

    let mut commissions = ArrowWriter::try_new(commissions_file, commissions_schema, None)?;
    commissions.write(&commissions_batch)?;
    commissions.close().unwrap();

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

    println!("Scraped data using {} web request(s).", web_request_count);

    Ok(())
}

struct QuestionData {
    questioners: Vec<String>,
    respondents: Vec<String>,
    topics: Vec<String>,
    discussion: String,
    dossier_ids: Vec<String>,
}

fn parse_commission_type(raw: &str) -> Commission {
    let raw = raw.trim().to_lowercase();
    if raw.contains("binnenlandse") {
        Commission::BinnenlandseZakenVeiligheidMigratieEnBestuurszaken
    } else if raw.contains("justitie") {
        Commission::Justitie
    } else if raw.contains("gezondheid") {
        Commission::GezondheidEnGelijkeKansen
    } else if raw.contains("economie") {
        Commission::EconomieConsumentenBeschermingEnDigitalisering
    } else if raw.contains("buitenlandse") {
        Commission::BuitenlandseBetrekkingen
    } else if raw.contains("mobiliteit") {
        Commission::MobiliteitOverheidsbedrijvenEnFederaleInstellingen
    } else if raw.contains("landsverdediging") {
        Commission::Landsverdediging
    } else if raw.contains("landsverdediging") {
        Commission::Landsverdediging
    } else if raw.contains("energie") {
        Commission::EnergieLeefmilieuEnKlimaat
    } else if raw.contains("sociale") {
        Commission::SocialeZakenWerkEnPensioenen
    } else if raw.contains("begroting") {
        Commission::FinancienEnBegroting
    } else if raw.contains("klimaatdialoog") {
        Commission::InterparlementaireKlimaatdialoog
    } else {
        Commission::Onbekend
    }
}

#[derive(Debug, Clone, Copy)]
enum Commission {
    Landsverdediging,
    Justitie,
    BuitenlandseBetrekkingen,
    FinancienEnBegroting,
    SocialeZakenWerkEnPensioenen,
    BinnenlandseZakenVeiligheidMigratieEnBestuurszaken,
    EconomieConsumentenBeschermingEnDigitalisering,
    MobiliteitOverheidsbedrijvenEnFederaleInstellingen,
    GezondheidEnGelijkeKansen,
    EnergieLeefmilieuEnKlimaat,
    InterparlementaireKlimaatdialoog,
    Onbekend,
}

impl fmt::Display for Commission {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Commission::BinnenlandseZakenVeiligheidMigratieEnBestuurszaken => {
                write!(f, "BinnenlandseZakenVeiligheidMigratieEnBestuurszaken")
            }

            Commission::Landsverdediging => write!(f, "Landsverdediging"),

            Commission::Justitie => write!(f, "Justitie"),

            Commission::BuitenlandseBetrekkingen => write!(f, "BuitenlandseBetrekkingen"),

            Commission::FinancienEnBegroting => write!(f, "FinancienEnBegroting"),

            Commission::SocialeZakenWerkEnPensioenen => write!(f, "SocialeZakenWerkEnPensioenen"),

            Commission::EconomieConsumentenBeschermingEnDigitalisering => {
                write!(f, "EconomieConsumentenBeschermingEnDigitalisering")
            }

            Commission::MobiliteitOverheidsbedrijvenEnFederaleInstellingen => {
                write!(f, "MobiliteitOverheidsbedrijvenEnFederaleInstellingen")
            }

            Commission::GezondheidEnGelijkeKansen => write!(f, "GezondheidEnGelijkeKansen"),

            Commission::EnergieLeefmilieuEnKlimaat => write!(f, "EnergieLeefmilieuEnKlimaat"),

            Commission::InterparlementaireKlimaatdialoog => {
                write!(f, "InterparlementaireKlimaatdialoog")
            }

            Commission::Onbekend => write!(f, "Onbekend"),
        }
    }
}

async fn scrape_commission(
    client: &ScrapingClient,
    session_id: u32,
    commission_id: u32,
    commission_ids: &mut Vec<String>,
    commission_session_ids: &mut Vec<String>,
    dates: &mut Vec<String>,
    times_of_day: &mut Vec<String>,
    start_times: &mut Vec<String>,
    end_times: &mut Vec<String>,
    commissions: &mut Vec<String>,
    chairs: &mut Vec<String>,
    question_ids: &mut Vec<String>,
    question_session_ids: &mut Vec<String>,
    question_meeting_ids: &mut Vec<String>,
    question_questioners: &mut Vec<String>,
    question_respondents: &mut Vec<String>,
    question_discussions: &mut Vec<String>,
    question_topics_nl: &mut Vec<String>,
    question_topics_fr: &mut Vec<String>,
    question_dossier_ids: &mut Vec<String>,
    web_request_count: &mut u32,
) -> Result<(), Box<dyn Error>> {
    println!("Scraping commission: {}", commission_id);
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
    let filename = format!(
        "data/sources/sessions/{}/meetings/commission/{}-{}.html",
        session_id, session_id, commission_id
    );
    let filepath = root.join(filename);

    // Download commission html if it does not exist yet.
    if !filepath.exists() {
        let url = format!(
            "https://www.dekamer.be/doc/CCRI/html/{}/ic{:03}x.html",
            session_id, commission_id
        );

        // Fetch and parse the HTML content.
        let response = client.get(&url).await?;
        *web_request_count += 1;

        if response.status().as_u16() == 404 {
            commission_session_ids.push(session_id.to_string());
            commission_ids.push(commission_id.to_string());
            times_of_day.push("404".to_string());
            dates.push("404".to_string());
            start_times.push("404".to_string());
            end_times.push("404".to_string());
            commissions.push("404".to_string());
            chairs.push("404".to_string());
            question_ids.push("404".to_string());
            question_session_ids.push("404".to_string());
            question_meeting_ids.push("404".to_string());
            question_questioners.push("404".to_string());
            question_respondents.push("404".to_string());
            question_discussions.push("404".to_string());
            question_topics_nl.push("404".to_string());
            question_topics_fr.push("404".to_string());
            question_dossier_ids.push("404".to_string());
            return Ok(());
        }

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
    // println!("MEETING ID: {}", commission_id);
    let date = extract_date_from_document(&document)?;

    let time_of_day = extract_time_of_day_from_document(&document)?;
    let start_time = extract_start_time_from_document(&document)?;
    let end_time = extract_end_time_from_document(&document)?;
    let chair = extract_chair_from_document(&document)?;
    let commission = extract_commission_from_document(&document)?;

    // EXTRACT QUESTIONS
    let span_selector = Selector::parse("span").unwrap();

    let mut previous_question_nl = String::new();
    let mut previous_question_fr = String::new();
    let mut previous_discussion = String::new();
    let mut question_id: i32 = 0;
    let mut found_questions = false;
    let mut processing_questions = false;
    // let mut collecting_subquestions = false;

    found_questions = true;
    processing_questions = true;

    for element in document.select(&Selector::parse("h1, h2, p").unwrap()) {
        let tag_name = element.value().name();

        // if tag_name == "h1" {
        //     let text = element
        //         .text()
        //         .collect::<Vec<_>>()
        //         .join(" ")
        //         .replace("\n", " ")
        //         .trim()
        //         .to_string();

        //     // Detect start of questions section.
        //     if text.contains("Mondelinge vragen")
        //         || text.contains("Vragen")
        //         || text.contains("Questions orales")
        //         || text.contains("Questions")
        //     {
        //         found_questions = true;
        //         processing_questions = true;
        //     } else if found_questions {
        //         // Avoid breaking on the second question header (which is just a translation).
        //         let is_question_header = text.contains("Mondelinge vragen")
        //             || text.contains("Vragen")
        //             || text.contains("Questions orales")
        //             || text.contains("Questions");

        //         if !is_question_header {
        //             // Save the grouped question.
        //             if !previous_question_nl.is_empty() || !previous_question_fr.is_empty() {
        //                 let question_nl = extract_question_data(
        //                     previous_question_nl.clone(),
        //                     previous_discussion.clone(),
        //                 )
        //                 .await?;
        //                 let question_fr = extract_question_data(
        //                     previous_question_fr.clone(),
        //                     previous_discussion.clone(),
        //                 )
        //                 .await?;

        //                 question_ids.push(question_id.to_string());
        //                 question_meeting_ids.push(commission_id.to_string());
        //                 question_session_ids.push(session_id.to_string());
        //                 question_discussions.push(question_nl.discussion.clone());
        //                 question_questioners.push(question_nl.questioners.join(","));
        //                 question_respondents.push(question_nl.respondents.join(","));
        //                 question_topics_nl.push(question_nl.topics.join(";"));
        //                 question_topics_fr.push(question_fr.topics.join(";"));
        //                 question_dossier_ids.push(question_nl.dossier_ids.join(","));
        //                 // question_id += 1;
        //                 previous_discussion.clear();
        //                 previous_question_nl.clear();
        //                 previous_question_fr.clear();
        //             }
        //             break;
        //         }
        //     }
        // }

        if !processing_questions {
            continue;
        }

        // Question.
        if tag_name == "h2" {
            // NOTE: Here we extract dutch/french language parts HOWEVER, they are not 100% reliable and may be incorrect.
            // NOTE: So, we analyze the text to be sure as well.
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

            let french_indicator_words = ["questions jointes", "question de"];
            let dutch_indicator_words = ["samengevoegde vragen", "toegevoegde vragen", "vraag van"];

            if let Some(span) = dutch_spans.last() {
                let raw_text = span.text().collect::<Vec<_>>().join(" ");
                let cleaned = clean_text(&raw_text).replace("\"", "\'");

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
            // NOTE: See https://www.dekamer.be/doc/CCRI/html/56/ic017x.html, here there is a grouped question that does not start with 'Samengevoegde' but has 'toegevoegde'.
            let is_group_start = found_text_nl.as_deref().map_or(false, |t| {
                t.starts_with("Samengevoegde") || t.contains("toegevoegde vragen")
            }) || found_text_fr
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
                        previous_question_nl.clone(),
                        previous_discussion.clone(),
                    )
                    .await?;
                    let question_fr = extract_question_data(
                        previous_question_fr.clone(),
                        previous_discussion.clone(),
                    )
                    .await?;

                    question_ids.push(question_id.to_string());
                    question_meeting_ids.push(commission_id.to_string());
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

    // Push last question if there is one
    if !previous_question_nl.is_empty() && !previous_question_fr.is_empty() {
        let question_nl =
            extract_question_data(previous_question_nl.clone(), previous_discussion.clone())
                .await?;
        let question_fr =
            extract_question_data(previous_question_fr.clone(), previous_discussion.clone())
                .await?;

        question_ids.push(question_id.to_string());
        question_meeting_ids.push(commission_id.to_string());
        question_session_ids.push(session_id.to_string());
        question_discussions.push(question_nl.discussion.clone());
        question_questioners.push(question_nl.questioners.join(","));
        question_respondents.push(question_nl.respondents.join(","));
        question_topics_nl.push(question_nl.topics.join(";"));
        question_topics_fr.push(question_fr.topics.join(";"));
        question_dossier_ids.push(question_nl.dossier_ids.join(","));

        question_id += 1;
    }

    commission_session_ids.push(session_id.to_string());
    commission_ids.push(commission_id.to_string());
    dates.push(date.to_string());
    times_of_day.push(time_of_day.to_string());
    start_times.push(start_time.to_string());
    end_times.push(end_time.to_string());
    commissions.push(commission.to_string());
    chairs.push(chair.to_string());

    Ok(())
}
fn extract_time_from_document(document: &Html, keywords: &[&str]) -> Option<String> {
    let span_selector = Selector::parse("span, p").unwrap();
    let re = Regex::new(r"(\d{1,2})\.(\d{2})\s*?uur").unwrap();

    let mut last_time: Option<String> = None;

    // Iterate through all spans.
    for span in document.select(&span_selector) {
        let text = span.text().collect::<Vec<_>>().join(" ").replace("\n", " ");

        // Check if any of the keywords are in the text
        if keywords.iter().any(|&keyword| text.contains(keyword)) {
            // println!("{}", text);
            // Try to capture the time pattern.
            if let Some(caps) = re.captures(&text) {
                // Update last_time with the most recent time match.
                last_time = Some(format!("{}h{}", &caps[1], &caps[2]));
            }
        }
    }

    last_time
}

fn extract_start_time_from_document(document: &Html) -> Result<String, Box<dyn Error>> {
    let keywords = [
        "De behandeling van de",
        "De behandeling van de vragen en de interpellatie vangt aan om",
        "De behandeling van de vragen en interpellaties vangt aan",
        "De behandeling van de vragen en van de interpellatie vangt aan om",
        "De openbare commissievergadering wordt geopend",
        "De vergadering wordt geopend",
        "De behandeling van de vragen vangt aan",
        "De gedachtewisseling vangt aan",
        "De behandeling van de interpellatie vangt",
    ];
    extract_time_from_document(document, &keywords)
        .ok_or_else(|| "No start time found matching the expected patterns".into())
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

fn extract_end_time_from_document(document: &Html) -> Result<String, Box<dyn Error>> {
    let keywords = [
        "De openbare commissievergadering wordt gesloten",
        "De gedachtewisseling met de ministers eindigt",
        "De behandeling van de vragen eindigt",
        "De gedachtewisseling eindigt",
        "De vergadering wordt gesloten",
        "De behandeling van de interpellatie eindigt",
        "De behandeling van de interpellaties eindigt",
    ];
    extract_time_from_document(document, &keywords)
        .ok_or_else(|| "No end time found matching the expected patterns".into())
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

pub fn extract_chair_from_document(document: &Html) -> Result<String, Box<dyn Error>> {
    let node_selector = Selector::parse("span, p").unwrap();

    // 1.  Grab everything that follows “voorgezeten door …” up to the dot (or EOL)
    //     We don’t try to be clever inside the regex; we’ll clean it up afterwards.
    let re_block = Regex::new(r"(?i)voorgezeten\s+door\s+([^\.]+?)\s*(?:\.|$)")?;

    // 2.  Titles we want to strip out.
    let re_titles = Regex::new(r"(?i)\b(?:de\s+)?(?:mevrouw|heer|mevrouwen|heren)\b")?;

    for node in document.select(&node_selector) {
        let text = node.text().collect::<Vec<_>>().join(" ").replace('\n', " ");

        if let Some(caps) = re_block.captures(&text) {
            // Raw chunk after “voorgezeten door …”
            let chunk = caps
                .get(1)
                .unwrap()
                .as_str()
                .replace('\u{00A0}', " ") // normalise NBSP
                .trim()
                .to_string();

            // Split on “ en ” and strip titles from each part
            let mut names: Vec<String> = Vec::new();
            for part in chunk.split(" en ") {
                let clean = re_titles
                    .replace_all(part.trim(), "") // remove title words
                    .trim() // trim leftover spaces
                    .to_string();
                if !clean.is_empty() {
                    names.push(clean);
                }
            }

            if !names.is_empty() {
                // println!("{}", names.join(","));
                return Ok(names.join(", "));
            }
        }
    }

    Err("Could not extract chair".into())
}

fn extract_commission_from_document(document: &Html) -> Result<String, Box<dyn Error>> {
    let table_selector = Selector::parse("table").unwrap();
    if let Some(first_table) = document.select(&table_selector).next() {
        let span_selector = Selector::parse("span").unwrap();
        if let Some(first_span) = first_table.select(&span_selector).next() {
            let raw = first_span
                .text()
                .collect::<Vec<_>>()
                .join(" ")
                .split_whitespace()
                .collect::<Vec<_>>()
                .join(" ");

            let commission = parse_commission_type(&raw);
            return Ok(commission.to_string());
        }
    }

    Err("Could not extract commission".into())
}

async fn extract_question_data(
    question_text: String,
    discussion_text: String,
) -> Result<QuestionData, Box<dyn Error>> {
    //  println!("TOPIC: {}", question_text);
    // FIXME: French issue, single ticks (fixed now?)
    //let regex = Regex::new(r#"(?m)(?:(?:Vraag van|Question de)\s)?([^\n]+?)\s+(?:aan|à)\s+([^\n]+?)\s*\(.*?\)\s*(?:over|sur)\s*'([^"']+?)'\s*\((\d{8}[A-Z])\)"#)?;
    // 10/11/2025: regex was (?m)(?:(?:Vraag van|Question de)\s)?([^\n]+?)\s+(?:aan|à)\s+([^\n]+?)\s*\(.*?\)\s*(?:over|sur)\s*["“'](.+?)["”']\s*\((\d{8}[A-Z])\)
    // but it failed for "Vraag van de heer Steven Coenegrachts aan de vice-eersteminister en minister van Werk, Economie en Landbouw over "Het banenverlies in de industrie" (nr. 6003263c)"
    // from commission meeting 107, because this does not have the info in brackets, also ID format changed...
    // so regex was modified
    // FIXME: STILL this question has an issue since the respondent is 'de vice-eersteminister en minister van Werk, Economie en Landbouw' but this is left for now..
    let regex = Regex::new(
        r#"(?m)(?:(?:Vraag van|Question de)\s)?([^\n]+?)\s+(?:aan|à)\s+([^\n]+?)(?:\s*\(.*?\))?\s*(?:over|sur)\s*["“'](.+?)["”']\s*\(?(?:nr\.?\s*)?(\d{6,8}[A-Za-z])\)?"#,
    )?;

    // let regex = Regex::new(r#"(?m)(?:(?:Vraag van|Question de)\s)?([^\n]+?)\s+(?:aan|à)\s+([^\n]+?)\s*\(.*?\)\s*(?:over|sur)\s*'([^"']+)"#)?;

    // (?m)(?:Vraag van\s)?([^\n]+?)\s+aan\s+([^\n]+?)\s*\(.*?\)\s*over\s*'([^']+)'",
    let mut questioners = Vec::new();
    let mut topics = Vec::new();
    let mut respondents = Vec::new();
    let mut dossier_ids = Vec::new();

    for capture in regex.captures_iter(&question_text) {
        let questioner = capture
            .get(1)
            .unwrap()
            .as_str()
            .trim()
            .replace("- ", "")
            .replace("de heer ", "") // remove titles
            .to_string();

        let respondent = capture.get(2).unwrap().as_str().trim().to_string();

        let topic = capture.get(3).unwrap().as_str().trim().to_string();

        let dossier_id = capture.get(4).unwrap().as_str().trim().to_string();

        questioners.push(questioner);

        if !respondents.contains(&respondent) {
            respondents.push(respondent);
        }

        dossier_ids.push(format!("{}{}", "Q", dossier_id));

        topics.push(topic);
    }

    Ok(QuestionData {
        questioners,
        respondents,
        topics,
        discussion: get_discussion_json(&discussion_text),
        dossier_ids,
    })
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
