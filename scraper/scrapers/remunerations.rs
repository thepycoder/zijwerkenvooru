#!/usr/bin/env rust-script
//! ```cargo
//! [dependencies]
//! reqwest = { version = "0.11", features = ["json"] }
//! scraper = "0.12"
//! tokio = { version = "1", features = ["full"] }
//! csv = "1.1"
//! serde = { version = "1.0", features = ["derive"] }
//! jsonwebtoken = "8"
//! chrono = "0.4"
//! headless_chrome = "1.0.17"
//! parquet = "54.3.0"
//! arrow = "54.3.0"
//! ```

use std::collections::HashSet;
use arrow::datatypes::{DataType, Field, Schema};
use headless_chrome::Browser;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::RowAccessor;
use scraper::{Html, Selector};
use serde::Deserialize;
use std::error::Error;
use std::fs;
use std::fs::{read_to_string, File};
use std::path::Path;
use std::sync::Arc;
use arrow::array::{RecordBatch, StringArray};
use parquet::arrow::ArrowWriter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Create browser.
    let browser = Browser::default()?;
    let tab = browser.new_tab()?;

    // Path to the members.parquet file and remuneration.parquet file
    let members_path = "./web/src/data/members.parquet";
    let remunerations_path = "./web/src/data/remunerations.parquet";

    // Create parquet file.
    let file = File::create(remunerations_path).unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("first_name", DataType::Utf8, false),
        Field::new("last_name", DataType::Utf8, false),
        Field::new("year", DataType::Utf8, false),
        Field::new("mandate", DataType::Utf8, false),
        Field::new("institute", DataType::Utf8, false),
        Field::new("remuneration_min", DataType::Utf8, false),
        Field::new("remuneration_max", DataType::Utf8, false),
    ]));

    // Scrape data.
    let mut first_names = vec![];
    let mut last_names = vec![];
    let mut years = vec![];
    let mut mandates = vec![];
    let mut institutes = vec![];
    let mut remunerations_min = vec![];
    let mut remunerations_max = vec![];

    // Read members from the members.parquet file
    let members = File::open(members_path).unwrap();
    let reader = SerializedFileReader::new(members).unwrap();
    let mut iter = reader.get_row_iter(None).unwrap();
    let mut processed_names = HashSet::new();
    let mut web_request_count = 0;

    // Loop through members.
    while let Some(row_result) = iter.next() {
        let row = row_result.unwrap();
        let first_name = row.get_string(2).unwrap();
        let last_name = row.get_string(3).unwrap();
        let full_name = format!("{} {}", first_name, last_name);

        if !processed_names.insert(full_name.clone()) {
            // println!("{}: remunerations already processed, skipping ...", full_name);
            continue;
        }

        let member = Member {
            first_name: first_name.to_string(),
            last_name: last_name.to_string(),
        };

        for year in 2018..=2023 {
            scrape_remuneration(
                &tab,
                &member.first_name,
                &member.last_name,
                year,
                &mut first_names,
                &mut last_names,
                &mut years,
                &mut mandates,
                &mut institutes,
                &mut remunerations_min,
                &mut remunerations_max,
                &mut web_request_count,
            )
            .await?;
        }
    }

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(first_names)),
            Arc::new(StringArray::from(last_names)),
            Arc::new(StringArray::from(years)),
            Arc::new(StringArray::from(mandates)),
            Arc::new(StringArray::from(institutes)),
            Arc::new(StringArray::from(remunerations_min)),
            Arc::new(StringArray::from(remunerations_max)),
        ],
    )?;

    let mut remunerations = ArrowWriter::try_new(file, schema, None)?;
    remunerations.write(&batch)?;
    remunerations.close().unwrap();

    println!("Scraped data using {} web request(s).", web_request_count);

    Ok(())
}

async fn scrape_remuneration(
    tab: &headless_chrome::Tab,
    first_name: &str,
    last_name: &str,
    year: u32,
    first_names: &mut Vec<String>,
    last_names: &mut Vec<String>,
    years: &mut Vec<String>,
    mandates: &mut Vec<String>,
    institutes: &mut Vec<String>,
    remunerations_min: &mut Vec<String>,
    remunerations_max: &mut Vec<String>,
    web_request_count: &mut u32
) -> Result<(), Box<dyn Error>> {
    let full_name = format!("{} {}", first_name, last_name);

    // Check if file already exists.

    let filename = format!("scraper/data/sources/remunerations/{}-{}-{}.html", last_name, first_name, year);
    let filepath = Path::new(&filename);

    // Download remuneration html if it does not exist yet.
    if !filepath.exists() {
        println!("Scraping remunerations...");
        let url = format!(
            "https://public.regimand.be/?mandatary={}&year={}",
            full_name, year
        );

        // println!("{}: missing remunerations, fetching ...", full_name);

        // Navigate to the page.
        tab.navigate_to(&url)?;
        *web_request_count += 1;
        tab.wait_for_element("kendo-autocomplete")?;

        let html_content = tab.get_content()?;
        if let Some(parent) = filepath.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        fs::write(&filepath, html_content).unwrap();
    }

    // Fetch and parse the HTML content.
    // println!("{}: found remunerations, processing ...", full_name);
    let content = read_to_string(filepath)?;
    let document = Html::parse_document(&content);

    let row_selector = Selector::parse("tbody tr").unwrap();
    let mandate_selector = Selector::parse("tbody td[aria-colindex='3']").unwrap();
    let institute_selector = Selector::parse("tbody td[aria-colindex='4'] button").unwrap();
    let remuneration_selector = Selector::parse("tbody td[aria-colindex='6']").unwrap();
    let no_result_selector = Selector::parse("tbody tr.k-grid-norecords td").unwrap();

    if document.select(&no_result_selector).any(|el| el.text().any(|text| text.contains("Geen resultaat gevonden"))) {
        println!("No results found for {} {}, skipping...", first_name, last_name);
        return Ok(());
    }

    // Iterate over each remuneration row.
    for row in document.select(&row_selector) {
        let mandate = row
            .select(&mandate_selector)
            .next()
            .map(|el| el.text().collect::<Vec<_>>().join(" "))
            .unwrap_or_else(|| String::from("Unknown"));

        let institute = row
            .select(&institute_selector)
            .next()
            .map(|el| el.text().collect::<Vec<_>>().join(" "))
            .unwrap_or_else(|| String::from("Unknown"));

        let remuneration = row
            .select(&remuneration_selector)
            .next()
            .map(|el| el.text().collect::<Vec<_>>().join(" "))
            .and_then(|raw_remuneration| clean_remuneration(&raw_remuneration))
            .unwrap_or_else(|| ("".to_string(), "".to_string()));

        first_names.push(first_name.to_string());
        last_names.push(last_name.to_string());
        years.push(year.to_string());
        mandates.push(mandate);
        institutes.push(institute);
        remunerations_min.push(remuneration.0);
        remunerations_max.push(remuneration.1);
    }

    Ok(())
}

fn clean_remuneration(raw_remuneration: &str) -> Option<(String, String)> {
    // Replace non-breaking spaces with normal space and remove unwanted characters

    if raw_remuneration.contains("Niet bezoldigd") {
        return Some(("0".to_string(), "0".to_string())); // Return 0 if it's "Not compensated"
    }

    let cleaned = raw_remuneration
        .replace("Afgerond op ", "")
        .replace("NBSP", " ") // Handling NBSP in the string
        .replace(&['€', '&', ' ', ',', ' '][..], "") // Remove currency symbol, commas, and spaces
        .replace(",", "."); // Replace commas with dots for proper float conversion

    // Check if it's a range or a single value
    if cleaned.contains("-") {
        // If it's a range, split by the dash
        let parts: Vec<&str> = cleaned.split('-').collect();
        if parts.len() == 2 {
            let start_value = parts[0].trim().parse::<f64>();
            let end_value = parts[1].trim().parse::<f64>();

            // If both values are valid, return the range as a string
            if let (Ok(start), Ok(end)) = (start_value, end_value) {
                return Some((start.to_string(), end.to_string()));//format!("{}-{}", start, end));
            }
        }
    } else {
        // Otherwise, it's a single value
        if let Ok(value) = cleaned.parse::<f64>() {
            return Some((value.to_string(), value.to_string()));
        }
    }

    // If we couldn't parse, return None
    None
}

#[derive(Deserialize)]
struct Member {
    first_name: String,
    last_name: String,
}
