#!/usr/bin/env rust-script
//! ```cargo
//! [dependencies]
//! reqwest = { version = "0.11", features = ["json"] }
//! scraper = "0.12"
//! tokio = { version = "1", features = ["full"] }
//! parquet = "54.3.0"
//! arrow = "54.3.0"
//! crawl = { path = "../crawl" }
//! ```

use crawl::client::ScrapingClient;
use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use scraper::{Html, Selector};
use std::error::Error;
use std::fs::{File, read_to_string};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs;

#[derive(Debug)]
struct CommissionIndex {
    name: String,
    ctype: String,
    url: String,
    file: PathBuf,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let client = ScrapingClient::new();

    // -----------------------
    // Paths
    // -----------------------
    let root = PathBuf::from("scraper/data/sources/commissions");
    let detail_dir = root.join("details");
    let index_path = root.join("commissions.html");
    let target_root = PathBuf::from("./web/src/data");

    let parquet_path = target_root.join("commissions.parquet");

    fs::create_dir_all(&detail_dir).await?;
    fs::create_dir_all(&root).await?;

    // -----------------------
    // Fetch index page
    // -----------------------
    let index_url =
        "https://www.dekamer.be/kvvcr/showpage.cfm?section=/none&language=nl&cfm=/site/wwwcfm/comm/LstCom.cfm";

    if !index_path.exists() {
        let resp = client.get(index_url).await?;
        let html = resp.text().await?;
        fs::write(&index_path, html).await?;
    }

    let index_html = read_to_string(&index_path)?;
    let document = Html::parse_document(&index_html);

    // -----------------------
    // Extract commission index
    // -----------------------
    use scraper::{Html, Selector};

    let mut commissions = Vec::new();
    let mut current_type = String::from("unknown");

    let selector = Selector::parse("div.linklist_0 > a, h4").unwrap();

    for element in document.select(&selector) {
        let tag_name = element.value().name();

        if tag_name == "h4" {
            // Update current type
            let text = element.text().collect::<String>().trim().to_string();
            if !text.is_empty() {
                current_type = text;
            }
            continue;
        }

        if tag_name == "a" {
            let Some(href) = element.value().attr("href") else { continue };

            // Only real commissions
            if !href.contains("/comm/com.cfm?com=") {
                continue;
            }

            let name = element.text().collect::<String>().trim().to_string();
            if name.is_empty() {
                continue;
            }

            let url = if href.starts_with("http") {
                href.to_string()
            } else {
                format!("https://www.dekamer.be/kvvcr/{}", href)
            };

            let safe = name
                .to_lowercase()
                .replace(' ', "_")
                .replace('/', "_");

            let file = detail_dir.join(format!("{safe}.html"));

            commissions.push(CommissionIndex {
                name: name.to_lowercase(),
                ctype: current_type.clone().to_lowercase(),
                url,
                file,
            });
        }
    }

    println!("Found {} commissions", commissions.len());



    // -----------------------
    // Prepare columns
    // -----------------------
    let mut names = Vec::new();
    let mut types = Vec::new();
    let mut chairs = Vec::new();
    let mut subchairs = Vec::new();
    let mut permanent = Vec::new();
    let mut replacements = Vec::new();

    // -----------------------
    // Scrape detail pages
    // -----------------------
    for c in &commissions {
        if !c.file.exists() {
            let resp = client.get(&c.url).await?;
            let html = resp.text().await?;
            fs::write(&c.file, html).await?;
        }

        let html = read_to_string(&c.file)?;
        let doc = Html::parse_document(&html);

        names.push(c.name.clone());
        types.push(c.ctype.clone());
        chairs.push(extract_members(&doc, "Voorzitter"));
        subchairs.push(extract_members(&doc, "Ondervoorzitter"));
        permanent.push(extract_members(&doc, "Vaste Leden"));
        replacements.push(extract_members(&doc, "Plaatsvervangers"));
    }

    if names.is_empty() {
        eprintln!("No commissions extracted — aborting Parquet write.");
        return Ok(());
    }

    // -----------------------
    // Write Parquet
    // -----------------------
    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("type", DataType::Utf8, false),
        Field::new("chairs", DataType::Utf8, false),
        Field::new("subchairs", DataType::Utf8, false),
        Field::new("permanent_members", DataType::Utf8, false),
        Field::new("replacement_members", DataType::Utf8, false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(names)) as ArrayRef,
            Arc::new(StringArray::from(types)),
            Arc::new(StringArray::from(chairs)),
            Arc::new(StringArray::from(subchairs)),
            Arc::new(StringArray::from(permanent)),
            Arc::new(StringArray::from(replacements)),
        ],
    )?;

    let file = File::create(&parquet_path)?;
    let mut writer = ArrowWriter::try_new(file, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;

    println!("Written {}", parquet_path.display());

    Ok(())
}

// -----------------------
// Helper
// -----------------------
fn extract_members(doc: &Html, role: &str) -> String {
    let p_sel = Selector::parse("p").unwrap();
    let b_sel = Selector::parse("b").unwrap();
    let a_sel = Selector::parse("a").unwrap();

    let mut names = Vec::new();
    let role = role.to_lowercase();

    for p in doc.select(&p_sel) {
        let Some(first_b) = p.select(&b_sel).next() else { continue };

        let section = first_b.text().collect::<String>().to_lowercase();
        if !section.contains(&role) {
            continue;
        }

        for a in p.select(&a_sel) {
            let name = a.text().collect::<String>().trim().to_string();
            if !name.is_empty() {
                names.push(name);
            }
        }
    }

    names.join(", ")
}
