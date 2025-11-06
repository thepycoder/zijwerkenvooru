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
use std::collections::HashSet;
use std::error::Error;
use std::fs::{File, read_to_string};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::fs;
use tokio::io::AsyncWriteExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Create client.
    let client = ScrapingClient::new();

    // NOTE: !!! Page 56 here is not same as 'today' so for session 56 also add today

    //https://www.dekamer.be/kvvcr/showpage.cfm?section=/depute&language=nl&cfm=cvlist54.cfm?legis=56&today=n

    let mut seen_members: HashSet<(i32, u64)> = HashSet::new();

    let mut sessions: Vec<((i32, bool), String)> = Vec::new();

    // First, push the active members
    sessions.push(
        (
            (56, true),
            "https://www.dekamer.be/kvvcr/showpage.cfm?section=/depute&language=nl&cfm=/site/wwwcfm/depute/cvlist54.cfm".to_string()
        )
    );

    // Then, extend with the others
    sessions.extend(
        vec![(56, false)]
            .into_iter()
            .map(|i| {
                (
                    i,
                    format!(
                        "https://www.dekamer.be/kvvcr/showpage.cfm?section=/depute&language=nl&cfm=cvlist54.cfm?legis={:02}&today=n",
                        i.0
                    )
                )
            })
    );

    // Create folders.
    let root = PathBuf::from("./web/src/data");
    let members_path = root.join("members.parquet");
    if let Some(parent) = members_path.parent() {
        fs::create_dir_all(parent).await?;
    }

    // Create parquet file.
    let members_file = File::create(members_path).unwrap();
    let members_schema = Arc::new(Schema::new(vec![
        Field::new("member_id", DataType::Utf8, false),
        Field::new("session_id", DataType::Utf8, false),
        Field::new("first_name", DataType::Utf8, false),
        Field::new("last_name", DataType::Utf8, false),
        Field::new("gender", DataType::Utf8, false),
        Field::new("date_of_birth", DataType::Utf8, false),
        Field::new("place_of_birth", DataType::Utf8, false),
        Field::new("language", DataType::Utf8, false),
        Field::new("constituency", DataType::Utf8, false),
        Field::new("party", DataType::Utf8, false),
        Field::new("fraction", DataType::Utf8, false),
        Field::new("email", DataType::Utf8, false),
        Field::new("active", DataType::Utf8, false),
        Field::new("start", DataType::Utf8, false),
    ]));

    // Scrape data.
    let mut member_ids = vec![];
    let mut session_ids = vec![];
    let mut first_names = vec![];
    let mut last_names = vec![];
    let mut genders = vec![];
    let mut birth_dates = vec![];
    let mut birth_places = vec![];
    let mut languages = vec![];
    let mut constituencies = vec![];
    let mut parties = vec![];
    let mut fractions = vec![];
    let mut emails = vec![];
    let mut actives = vec![];
    let mut starts = vec![];

    let mut web_request_count = 0;

    for session in sessions {
        // Check if file already exists.
        let filename = format!("scraper/data/sources/sessions/{:?}/index.html", session.0);
        let filepath = Path::new(&filename);

        // Download members html if it does not exist yet.
        if !filepath.exists() {
            let response = client.get(&session.1).await?;
            web_request_count += 1;
            let content = response.text().await?;
            if let Some(parent) = filepath.parent() {
                std::fs::create_dir_all(parent).unwrap();
            }
            std::fs::write(filepath, content).unwrap();
        }

        // Fetch and parse the HTML content.
        let content = read_to_string(filepath)?;
        let document = Html::parse_document(&content);

        extract_members(
            &client,
            document,
            session.0,
            &mut session_ids,
            &mut member_ids,
            &mut first_names,
            &mut last_names,
            &mut genders,
            &mut birth_dates,
            &mut birth_places,
            &mut languages,
            &mut constituencies,
            &mut parties,
            &mut fractions,
            &mut emails,
            &mut actives,
            &mut starts,
            &mut web_request_count,
            &mut seen_members,
        )
        .await?;
    }

    // Add hardcoded members.
    append_hardcoded_members(
        &mut session_ids,
        &mut member_ids,
        &mut first_names,
        &mut last_names,
        &mut genders,
        &mut birth_dates,
        &mut birth_places,
        &mut languages,
        &mut constituencies,
        &mut parties,
        &mut fractions,
        &mut emails,
        &mut actives,
        &mut starts,
        &mut seen_members,
    );

    let batch = RecordBatch::try_new(
        members_schema.clone(),
        vec![
            Arc::new(StringArray::from(member_ids)) as ArrayRef,
            Arc::new(StringArray::from(session_ids)),
            Arc::new(StringArray::from(first_names)),
            Arc::new(StringArray::from(last_names)),
            Arc::new(StringArray::from(genders)),
            Arc::new(StringArray::from(birth_dates)),
            Arc::new(StringArray::from(birth_places)),
            Arc::new(StringArray::from(languages)),
            Arc::new(StringArray::from(constituencies)),
            Arc::new(StringArray::from(parties)),
            Arc::new(StringArray::from(fractions)),
            Arc::new(StringArray::from(emails)),
            Arc::new(StringArray::from(actives)),
            Arc::new(StringArray::from(starts)),
        ],
    )?;

    let mut members = ArrowWriter::try_new(members_file, members_schema, None)?;
    members.write(&batch)?;
    members.close().unwrap();

    println!("Scraped data using {} web request(s).", web_request_count);

    Ok(())
}

#[derive(Hash)]
struct Member {
    session_id: i32,
    first_name: String,
    last_name: String,
}

async fn extract_members(
    client: &ScrapingClient,
    parent_document: Html,
    session_info: (i32, bool),
    session_ids: &mut Vec<String>,
    member_ids: &mut Vec<String>,
    first_names: &mut Vec<String>,
    last_names: &mut Vec<String>,
    genders: &mut Vec<String>,
    birth_dates: &mut Vec<String>,
    birth_places: &mut Vec<String>,
    languages: &mut Vec<String>,
    constituencies: &mut Vec<String>,
    parties: &mut Vec<String>,
    fractions: &mut Vec<String>,
    emails: &mut Vec<String>,
    actives: &mut Vec<String>,
    starts: &mut Vec<String>,
    web_request_count: &mut u32,
    seen_members: &mut HashSet<(i32, u64)>,
) -> Result<(), Box<dyn Error>> {
    let total_rows = parent_document
        .select(&Selector::parse("tr").unwrap())
        .count();

    // Property selectors.
    let name_selector = Selector::parse("tr a[href*='cvview54.cfm'] > b").unwrap();
    let party_selector = Selector::parse("tr a[href*='cvlist54.cfm']").unwrap();
    let details_link_selector = Selector::parse("tr a[href*='cvview54.cfm']").unwrap();
    let email_selector = Selector::parse("tr a[href*='mailto:']").unwrap();
    let language_selector = Selector::parse("p i").unwrap();

    let row_selector = Selector::parse("tr").unwrap();
    let rows = parent_document.select(&row_selector);

    // Iterate over each row.
    let mut processed_rows = 0;
    for row in rows
    {
        // Only process rows that actually contain a member link
        if extract_from_row(&row, &name_selector, None).is_none() {
            continue; // skip disclaimer/footer or empty rows
        }

        // Extract name.
        let name = match extract_from_row(&row, &name_selector, None) {
            None => "".to_string(),
            Some(name) => {
                let mut parts: Vec<&str> = name.split_whitespace().collect();
                if parts.len() > 1 {
                    let first_name = parts.pop().unwrap(); // Take the last word as the first name
                    format!("{} {}", first_name, parts.join(" ")) // Join the rest as the last name
                } else {
                    name // If there's only one word, return it as is
                }
            }
        };

        let mut fraction =
            extract_from_row(&row, &party_selector, None).unwrap_or_else(|| "".to_string());
        let link = extract_from_row(&row, &details_link_selector, Some("href"))
            .unwrap_or_else(|| "unknown".to_string());
        let email = match extract_from_row(&row, &email_selector, None) {
            None => "".to_string(),
            Some(email) => email.chars().rev().collect::<String>(),
        };

        // Check if file already exists.
        let filename = format!("scraper/data/sources/members/{}/details.html", name);
        let filepath = Path::new(&filename);

        // Download details html if it does not exist yet.
        if !filepath.exists() {
            let member_page_url = format!("https://www.dekamer.be/kvvcr/{}", link);

            // Fetch and parse the HTML content.
            let content = client.get(&member_page_url).await?.text().await?;
            *web_request_count += 1;
            if let Some(parent) = filepath.parent() {
                std::fs::create_dir_all(parent).unwrap();
            }
            std::fs::write(filepath, content).unwrap();
        }

        // Fetch and parse the HTML content.
        let content = read_to_string(filepath)?;
        let member_document = Html::parse_document(&content);

        // Extract language.
        let language = match extract_sibling_text(&member_document, &language_selector, "Taal") {
            None => "".to_string(),
            Some(language) => dutch_language_to_language_code(language.as_str())
                .unwrap()
                .to_ascii_lowercase(),
        };

        let patterns = [
            r"Lid van de (.+?)-fractie",
            r"Lid-van de (.+?)-fractie",
            r"Voorzitster van de (.+?)-fractie",
            r"Voorzitter van de (.+?)-fractie",
        ];

        if fraction.is_empty() {
            let text = member_document
                .select(&Selector::parse("p").unwrap())
                .find(|el| {
                    el.text().any(|t| {
                        t.contains("olksvertegenwoordiger")
                            && (t.contains("arrondissement") || t.contains("kieskring"))
                    })
                })
                .map(|el| el.text().collect::<String>())
                .unwrap_or_default();

            if text.contains("Behoort niet tot een erkende politieke fractie.") {
                fraction = "independent".to_string();
            } else {
                for pattern in patterns.iter() {
                    if let Ok(re) = Regex::new(pattern) {
                        if let Some(caps) = re.captures(&text) {
                            if let Some(m) = caps.get(1) {
                                fraction = m.as_str().to_string();
                                break;
                            }
                        }
                    }
                }
            }
        }

        let mut party = member_document
            .select(&Selector::parse("p").unwrap())
            .find(|el| el.text().any(|t| t.contains("olksvertegenwoordiger") && (t.contains("arrondissement") || t.contains("kieskring"))))
            .and_then(|el| {
                let text = el.text().collect::<String>();
                let re = Regex::new(r"[v|V]olksvertegenwoordiger van het\s*([\w\s&-]+?)\s*(?:voor de|sedert|voor het|\.)").unwrap();
                re.captures(&text)
                    .and_then(|caps| caps.get(1).map(|m| m.as_str().trim().to_string()))
            })
            .unwrap_or_else(|| "".to_string());

        if party.is_empty() {
            party = member_document
                .select(&Selector::parse("p").unwrap())
                .find(|el| {
                    el.text().any(|t| {
                        t.contains("olksvertegenwoordiger")
                            && (t.contains("arrondissement") || t.contains("kieskring"))
                    })
                })
                .and_then(|el| {
                    let text = el.text().collect::<String>();
                    let re = Regex::new(
                        r"\(([^)]+?)\)\s*(?:voor de kieskring|van het|voor het arrondissement)",
                    )
                    .unwrap();
                    re.captures(&text)
                        .and_then(|caps| caps.get(1).map(|m| m.as_str().trim().to_string()))
                })
                .unwrap_or_else(|| "".to_string());
        }

        // Extract birthplace.
        // Extract birthplace.
        let birth_place = member_document
            .select(&Selector::parse("p").unwrap())
            .find(|el| el.text().any(|t| t.contains("Geboren te")))
            .map(|el| {
                let text = el.text().collect::<String>();
                if let Some(after) = text.split("Geboren te").nth(1) {
                    if after.contains("op") {
                        after
                            .split("op")
                            .next()
                            .unwrap_or("")
                            .trim()
                            .to_string()
                    } else {
                        after
                            .split(|c| c == '.' || c == '|' || c == '\n')
                            .next()
                            .unwrap_or("")
                            .trim()
                            .to_string()
                    }
                } else {
                    "".to_string()
                }
            })
            .unwrap_or_default();


        // Extract birthdate.
        let birth_date_raw = member_document
            .select(&Selector::parse("p").unwrap())
            .find(|el| el.text().any(|t| t.contains("Geboren te")))
            .map(|el| {
                let text = el.text().collect::<String>();
                text.split("Geboren te")
                    .nth(1)
                    .and_then(|s| s.split("op").nth(1))
                    .map(|s| s.split('.').next().unwrap_or_default().trim().to_string())
                    .unwrap_or_default()
            })
            .unwrap_or_default();

        let birth_date = if !birth_date_raw.is_empty() {
            let parts: Vec<&str> = birth_date_raw.split_whitespace().collect();
            if parts.len() == 3 {
                if let (Ok(day), Some(month), Ok(year)) = (
                    parts[0].parse::<u32>(),
                    dutch_month_to_number(parts[1]),
                    parts[2].parse::<i32>(),
                ) {
                    NaiveDate::from_ymd_opt(year, month, day)
                        .map(|d| d.format("%Y-%m-%d").to_string())
                        .unwrap_or_default()
                } else {
                    "".to_string()
                }
            } else {
                "".to_string()
            }
        } else {
            "".to_string()
        };

        // Extract start data.
        // Extract start date (e.g., sedert 5 december 2024)
        let start_date_text = member_document
            .select(&Selector::parse("p").unwrap())
            .find(|el| el.text().any(|t| t.contains("sedert")))
            .map(|el| el.text().collect::<String>())
            .unwrap_or_default();

        let default_start_date = "2024-06-09".to_string();
        let start_date = if let Some(sedert_pos) = start_date_text.find("sedert") {
            let after_sedert = &start_date_text[sedert_pos + 6..]; // skip 'sedert'
            let re = Regex::new(r"(\d{1,2}) (\w+) (\d{4})").unwrap();
            if let Some(caps) = re.captures(after_sedert) {
                let day = caps.get(1).unwrap().as_str().parse::<u32>().unwrap_or(0);
                let month_str = caps.get(2).unwrap().as_str();
                let year = caps.get(3).unwrap().as_str().parse::<i32>().unwrap_or(0);
                if let Some(month) = dutch_month_to_number(month_str) {
                    NaiveDate::from_ymd_opt(year, month, day)
                        .map(|d| d.format("%Y-%m-%d").to_string())
                        .unwrap_or_default()
                } else {
                    default_start_date
                }
            } else {
                default_start_date
            }
        } else {
            default_start_date
        };

        // Extract constituency.
        let constituency = member_document
            .select(&Selector::parse("p").unwrap())
            .find(|el| {
                el.text().any(|t| {
                    t.contains("olksvertegenwoordiger")
                        && (t.contains("arrondissement") || t.contains("kieskring"))
                })
            })
            .and_then(|el| {
                let text = el.text().collect::<String>();
                let re = Regex::new(
                    r"(?:voor de kieskring|voor het arrondissement)\s+([A-Za-z0-9\s-]+)",
                )
                .unwrap();
                // Capture the constituency name first
                re.captures(&text)
                    .and_then(|caps| caps.get(1).map(|m| m.as_str().trim().to_string()))
                    .map(|constituency| {
                        // Trim out anything after "sedert" or a period to ensure we get just the name
                        constituency
                            .split_whitespace()
                            .take_while(|&word| {
                                word != "sedert" && word != "van" && !word.ends_with('.')
                            })
                            .collect::<Vec<&str>>()
                            .join(" ")
                    })
            })
            .unwrap_or_else(|| "".to_string());

        scrape_image(&client, &name, member_document, web_request_count).await?;

        let parts: Vec<&str> = name.split_whitespace().collect();
        let first_name = parts[0].to_string();
        let last_name = parts[1..].join(" ");

        let gender = "".to_string();

        let member = Member {
            session_id: session_info.0.clone(),
            first_name: first_name.clone(),
            last_name: last_name.clone(),
        };

        let member_id = calculate_hash(&member);

        let key = (session_info.0, member_id);

        if seen_members.contains(&key) {
            continue; // Skip duplicate
        }
        seen_members.insert(key);

        // Now push safely
        member_ids.push(member_id.to_string());
        session_ids.push(session_info.0.to_string());
        first_names.push(first_name);
        last_names.push(last_name);
        genders.push(gender);
        birth_dates.push(birth_date);
        birth_places.push(birth_place);
        languages.push(language);
        constituencies.push(constituency);
        parties.push(party);
        fractions.push(fraction);
        emails.push(email);
        actives.push(session_info.1.to_string());
        starts.push(start_date);

        processed_rows += 1;

        if processed_rows % 10 == 0 {
            println!("Processed {}/{} rows...", processed_rows, total_rows);
        }
    }
    Ok(())
}

fn calculate_hash<T: Hash>(t: &T) -> u64 {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}

async fn scrape_image(
    client: &ScrapingClient,
    name: &String,
    member_document: Html,
    web_request_count: &mut u32,
) -> Result<(), Box<dyn Error>> {
    let image_selector = Selector::parse("img[alt='Picture']").unwrap();

    if let Some(img_element) = member_document.select(&image_selector).next() {
        if let Some(img_src) = img_element.value().attr("src") {
            let img_url = format!("https://www.dekamer.be{}", img_src);

            // Convert name to a file-safe format
            let filename = slugify_name(name.as_str());

            let img_path = format!("./web/src/content/members/img/{}.jpg", filename);

            // Check if the image already exists
            if Path::new(&img_path).exists() {
                println!("Image already exists: {}", img_path);
            } else {
                // Ensure directory exists
                let img_dir = Path::new("./web/src/content/members/img");
                if !img_dir.exists() {
                    fs::create_dir_all(img_dir).await?;
                }

                // Download and save image
                let img_bytes = client.get(&img_url).await?.bytes().await?;
                *web_request_count += 1;
                let mut file = File::create(&img_path).unwrap();
                file.write_all(&img_bytes)?;
                println!("Saved image: {}", img_path);
            }
        }
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

fn extract_sibling_text(
    document: &Html,
    selector: &Selector,
    contains_text: &str,
) -> Option<String> {
    document
        .select(selector)
        .find(|el| el.text().any(|t| t.contains(contains_text)))
        .and_then(|el| {
            el.next_sibling()
                .and_then(|sib| sib.value().as_text().map(|t| t.trim().to_string()))
        })
}

fn append_hardcoded_members(
    session_ids: &mut Vec<String>,
    member_ids: &mut Vec<String>,
    first_names: &mut Vec<String>,
    last_names: &mut Vec<String>,
    genders: &mut Vec<String>,
    birth_dates: &mut Vec<String>,
    birth_places: &mut Vec<String>,
    languages: &mut Vec<String>,
    constituencies: &mut Vec<String>,
    parties: &mut Vec<String>,
    fractions: &mut Vec<String>,
    emails: &mut Vec<String>,
    actives: &mut Vec<String>,
    starts: &mut Vec<String>,
    seen_members: &mut HashSet<(i32, u64)>,
) {
    let hardcoded_members = vec![
        (
            56,
            "Rob".to_string(),
            "Beenders".to_string(),
            "M".to_string(),
            "1979-04-26".to_string(),
            "Bree".to_string(),
            "nl".to_string(),
            "Limburg".to_string(),
            "Vooruit".to_string(),
            "".to_string(),
            "".to_string(),
            "false".to_string(),
        ),
        (
            56,
            "Jan".to_string(),
            "Jambon".to_string(),
            "M".to_string(),
            "1960-04-26".to_string(),
            "Genk".to_string(),
            "nl".to_string(),
            "Antwerpen".to_string(),
            "N-VA".to_string(),
            "".to_string(),
            "".to_string(),
            "false".to_string(),
        ),
        (
            56,
            "Bernard".to_string(),
            "Quintin".to_string(),
            "M".to_string(),
            "1971-01-01".to_string(),
            "".to_string(),
            "fr".to_string(),
            "".to_string(),
            "MR".to_string(),
            "".to_string(),
            "".to_string(),
            "false".to_string(),
        ),
        (
            56,
            "Eléonore".to_string(),
            "Simonet".to_string(),
            "F".to_string(),
            "1997-11-26".to_string(),
            "Luik".to_string(),
            "fr".to_string(),
            "".to_string(),
            "MR".to_string(),
            "".to_string(),
            "".to_string(),
            "false".to_string(),
        ),
        (
            56,
            "Nicole".to_string(),
            "de Moor".to_string(),
            "F".to_string(),
            "1984-01-18".to_string(),
            "Sint-Niklaas".to_string(),
            "nl".to_string(),
            "Brussel-Hoofdstad".to_string(),
            "CD&V".to_string(),
            "".to_string(),
            "".to_string(),
            "false".to_string(),
        ),
    ];

    for (
        session_id,
        first,
        last,
        gender,
        birth_date,
        birth_place,
        lang,
        constituency,
        party,
        fraction,
        email,
        active,
    ) in hardcoded_members
    {
        let member = Member {
            session_id,
            first_name: first.clone(),
            last_name: last.clone(),
        };

        let member_id = calculate_hash(&member);
        let key = (session_id, member_id);

        if seen_members.contains(&key) {
            continue;
        }

        seen_members.insert(key);

        session_ids.push(session_id.to_string());
        member_ids.push(member_id.to_string());
        first_names.push(first);
        last_names.push(last);
        genders.push(gender);
        birth_dates.push(birth_date);
        birth_places.push(birth_place);
        languages.push(lang);
        constituencies.push(constituency);
        parties.push(party);
        fractions.push(fraction);
        emails.push(email);
        actives.push(active);
        starts.push("NA".to_string());
    }
}
