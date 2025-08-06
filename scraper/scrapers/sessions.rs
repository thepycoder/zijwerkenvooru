#!/usr/bin/env rust-script
//! This is a regular crate doc comment, but it also contains a partial
//! Cargo manifest.  Note the use of a *fenced* code block, and the
//! `cargo` "language".
//!
//! ```cargo
//! [dependencies]
//! reqwest = { version = "0.11", features = ["json"] }
//! scraper = "0.12"
//! tokio = { version = "1", features = ["full"] }
//! csv = "1.1"
//! chrono = "0.4"
//! regex = "1"
//! crawl = { path = "../../crawl" }
//! parquet = "54.3.0"
//! arrow = "54.3.0"
//! ```

use arrow::array::StringArray;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use crawl::client::ScrapingClient;
use parquet::arrow::ArrowWriter;
use scraper::{ElementRef, Html, Selector};
use std::error::Error;
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs;
use tokio::io::AsyncWriteExt;

const URL: &'static str = "https://www.dekamer.be/kvvcr/showpage.cfm?section=/depute&language=nl&cfm=/site/wwwcfm/depute/cvlist54.cfm";

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Create client.
    let client = ScrapingClient::new();

    // Create folders.
    let root = PathBuf::from("./data");
    let sessions_path = root.join("sessions/sessions.parquet");
    if let Some(parent) = sessions_path.parent() {
        fs::create_dir_all(parent).await?;
    }

    // Scrape data.
    let mut session_ids = vec![];
    let mut start_dates_ = vec![];
    let mut end_dates = vec![];

    // Fetch and parse the HTML content.
    let response = client.get(&URL).await?;
    let content = response.text().await?;
    let document = Html::parse_document(&content);

    extract_sessions(
        document,
        &mut session_ids,
        &mut start_dates_,
        &mut end_dates,
    )
    .await?;

    // Create parquet file.
    let file = File::create(sessions_path).unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("session_id", DataType::Utf8, false),
        Field::new("start_date", DataType::Utf8, false),
        Field::new("end_date", DataType::Utf8, false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(session_ids)),
            Arc::new(StringArray::from(start_dates_)),
            Arc::new(StringArray::from(end_dates)),
        ],
    )?;

    let mut sessions = ArrowWriter::try_new(file, schema, None)?;
    sessions.write(&batch)?;
    sessions.close().unwrap();

    Ok(())
}
async fn extract_sessions(
    document: Html,
    session_ids: &mut Vec<String>,
    start_dates: &mut Vec<String>,
    end_dates: &mut Vec<String>,
) -> Result<(), Box<dyn Error>> {

    // Extract sessions.
    for session in document.select(&Selector::parse("div a[href*='showpage.cfm']").unwrap())
    {
        // Extract session ID + date.
        if let Some(href) = extract_from_row(&session, &Selector::parse("a").unwrap(), Some("href")) {
            if let Some(session_id) = href.split("legis=").nth(1).and_then(|s| s.split('&').next()) {
                // Extract the text content (e.g., "56 (2024- )" or "55 (2019-2024)")
                if let Some(session_text) = extract_from_row(&session, &Selector::parse("a").unwrap(), None) {
                    // Extract start and end dates from the session text (format: "YYYY-YYYY")
                    let dates: Vec<&str> = session_text.split('(').nth(1).unwrap_or("").split(')').next().unwrap_or("").split('-').collect();

                    // If there are start and end dates, we can split them
                    if dates.len() == 2 {
                        let start_date = dates[0].trim().to_string();
                        let end_date = dates[1].trim().to_string();

                        // Push extracted information to the vectors
                        session_ids.push(session_id.to_string());
                        start_dates.push(start_date);
                        end_dates.push(end_date);
                    }
                }
            }
        }

        // TODO: get exact start/dates of sessions? The member detail page has this info
        //CV : Zittingsperiode 56 (09.06.2024 - ....)


    }
    Ok(())
}

fn extract_from_row(row: &ElementRef, selector: &Selector, attr: Option<&str>) -> Option<String> {
    row.select(selector).next().map(|el| {
        if let Some(attr_name) = attr {
            el.value().attr(attr_name).unwrap_or_default().to_string()
        } else {
            el.text().collect::<String>().trim().to_string()
        }
    })
}
