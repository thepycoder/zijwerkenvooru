#!/usr/bin/env rust-script
//! ```cargo
//! [dependencies]
//! reqwest = { version = "0.11", features = ["json"] }
//! scraper = "0.12"
//! tokio = { version = "1", features = ["full"] }
//! csv = "1.1"
//! chrono = "0.4"
//! regex = "1"
//! crawl = { path = "../crawl" }
//! parquet = "54.3.0"
//! arrow = "54.3.0"
//! ```

use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use chrono::NaiveDate;
use crawl::client::ScrapingClient;
use crawl::utils::{dutch_language_to_language_code, dutch_month_to_number, slugify_name};
use parquet::arrow::ArrowWriter;
use regex::Regex;
use scraper::{ElementRef, Html, Selector};
use std::error::Error;
use std::fs::{read_to_string, File};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::fs;
use tokio::io::AsyncWriteExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Create folders.
    let root = PathBuf::from("web/src/data");
    let lobby_path = root.join("lobby.parquet");
    if let Some(parent) = lobby_path.parent() {
        fs::create_dir_all(parent).await?;
    }

    // Create parquet file.
    let lobby_file = File::create(lobby_path).unwrap();
    let lobby_schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("contacts", DataType::Utf8, false),
        Field::new("interests", DataType::Utf8, false),
        Field::new("url", DataType::Utf8, false),
    ]));

    // Scrape data.
    let mut names = vec![];
    let mut contacts = vec![];
    let mut interests = vec![];
    let mut urls = vec![];

    // Check if file already exists.
    let filename = format!("scraper/data/sources/lobby/lobbyregister.html");
    let filepath = Path::new(&filename);

    if !filepath.exists() {
        // DOWNLOAD AND CONVERT TO PDF TO HTML?
    }

    // Fetch and parse the HTML content.
    let content = read_to_string(filepath)?;
    let document = Html::parse_document(&content);

    extract_lobby(
        document,
        &mut names,
        &mut contacts,
        &mut interests,
        &mut urls
    )
    .await?;


    let batch = RecordBatch::try_new(
        lobby_schema.clone(),
        vec![
            Arc::new(StringArray::from(names)) as ArrayRef,
            Arc::new(StringArray::from(contacts)),
            Arc::new(StringArray::from(interests)),
            Arc::new(StringArray::from(urls)),
        ],
    )?;

    let mut lobby = ArrowWriter::try_new(lobby_file, lobby_schema, None)?;
    lobby.write(&batch)?;
    lobby.close().unwrap();

    Ok(())
}


async fn extract_lobby(
    parent_document: Html,
    names: &mut Vec<String>,
    contacts: &mut Vec<String>,
    interests: &mut Vec<String>,
    urls: &mut Vec<String>,
) -> Result<(), Box<dyn Error>> {
    let total_rows = parent_document.select(&Selector::parse("tr").unwrap()).count();

    // Iterate over each row.
    let mut processed_rows = 0;
    for (_, row) in parent_document
        .select(&Selector::parse("tr").unwrap())
        .skip(1)
        .take(total_rows - 1)
        .enumerate()
    {
        let name = extract_from_row(&row, 0);
        let contact = extract_from_row(&row, 1);
        let interest = extract_from_row(&row, 2);
        let url = extract_from_row(&row, 3);

        names.push(name.to_string());
        contacts.push(contact.to_string());
        interests.push(interest.to_string());
        urls.push(url.to_string());

        processed_rows += 1;
    }
    Ok(())
}


fn extract_from_row(row: &ElementRef, cell_index: usize) -> String {
    let cells: Vec<_> = row.select(&Selector::parse("td").unwrap()).collect();
    if cell_index < cells.len() {
        cells[cell_index]
            .text()
            .collect::<Vec<_>>()
            .join(" ")
            .trim()
            .to_string()
    } else {
        String::new()
    }
}

