use arrow::array::Array;
use arrow::array::StringArray;
use atrium_api::app::bsky::feed::post::{RecordData, RecordEmbedRefs, ReplyRefData};
use atrium_api::types::string::Datetime;
use bsky_sdk::rich_text::RichText;
use bsky_sdk::BskyAgent;
use chrono::Datelike;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::{
    fs::{self, File},
    path::Path,
};
use std::error::Error;
use std::ffi::OsStr;
use std::num::NonZeroU64;
use std::path::PathBuf;
use std::time::Duration;
use atrium_api::app::bsky::embed::images::ImageData;
use atrium_api::types::Union;
use dotenv::dotenv;
use unicode_segmentation::UnicodeSegmentation;
use headless_chrome::{Browser, LaunchOptionsBuilder};
use headless_chrome::protocol::cdp::Page::Viewport;
use parquet::data_type::AsBytes;

fn truncate_to_graphemes(s: &str, count: usize) -> String {
    let graphemes: Vec<&str> = UnicodeSegmentation::graphemes(s, true).collect();
    if graphemes.len() > count {
        graphemes[..(count-1)].join("") + "…"
    } else {
        s.to_string()
    }
}

fn hash_text(input: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(input.as_bytes());
    format!("{:x}", hasher.finalize())
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum BskyReply {
    Vote(BskyReplyVote),
    Question(BskyReplyQuestion),
}

#[derive(Debug, Serialize, Deserialize)]
struct BskyPost {
    pub id: String,
    pub date: String,
    pub uri: String,
    pub replies: Vec<BskyReply>,
}

#[derive(Debug, Serialize, Deserialize)]
struct BskyReplyVote  {
    pub hash: String,
    pub topic: String,
    pub uri: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct BskyReplyQuestion  {
    pub hash: String,
    pub topic: String,
    pub uri: String,
    pub questioners: Vec<String>,
    pub respondents: Vec<String>,
}

fn parse_dutch_date(date_str: &str) -> Option<chrono::NaiveDate> {
    let months = [
        "", "januari", "februari", "maart", "april", "mei", "juni",
        "juli", "augustus", "september", "oktober", "november", "december",
    ];

    let parts: Vec<&str> = date_str.split_whitespace().collect();
    if parts.len() != 3 {
        return None;
    }

    let day = parts[0].parse::<u32>().ok()?;
    let month_str = parts[1];
    let year = parts[2].parse::<i32>().ok()?;

    let month = months.iter().position(|&m| m == month_str)?;
    chrono::NaiveDate::from_ymd_opt(year, month as u32, day)
}


fn format_dutch_date(date_str: &str) -> Option<String> {
    let months = [
        "", "januari", "februari", "maart", "april", "mei", "juni",
        "juli", "augustus", "september", "oktober", "november", "december",
    ];

    if let Ok(date) = chrono::NaiveDate::parse_from_str(date_str, "%Y-%m-%d") {
        let day = date.day();
        let month = months.get(date.month() as usize)?;
        let year = date.year();
        Some(format!("{} {} {}", day, month, year))
    } else {
        None
    }
}


fn read_string_column(file_path: &PathBuf, col_name: &str) -> anyhow::Result<Vec<String>> {
    let file = File::open(file_path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let reader = builder.build()?;
    let mut result = Vec::new();

    for batch in reader {
        let batch = batch?;
        let column = batch
            .column_by_name(col_name)
            .expect(&format!("Missing '{}'", col_name))
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..column.len() {
            result.push(column.value(i).to_string());
        }
    }
    Ok(result)
}

fn get_data_dir() -> PathBuf {
    // CARGO_MANIFEST_DIR points to poster/
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));

    // From poster/ go up to repo root, then into web/src/data
    manifest_dir
        .parent().unwrap() // go to root
        .join("web/src/data")
}

fn get_log_file_path(filename: &str) -> PathBuf {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    manifest_dir.join(filename)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    dotenv().ok();
    let username = std::env::var("BSKY_USERNAME").expect("Missing BSKY_USERNAME");
    let password = std::env::var("BSKY_PASSWORD").expect("Missing BSKY_PASSWORD");
    let meeting_id_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../scraper/current_plenary_id.txt");
    let last_meeting_id = fs::read_to_string(&meeting_id_path)?;
    let target_meeting_id = last_meeting_id.trim();

    // Hardcoded name to handle mapping.
    let name_to_handle: HashMap<&str, &str> = HashMap::from([
        ("Staf Aerts", "@stafaerts.be"),
        ("Meyrem Almaci", "meyremalmaci.bsky.social"),
        ("Khalil Aouasti", "@khalilaouasti.bsky.social"),
        ("Ridouane Chahid", "@ridouanechahid.bsky.social"),
        ("Steven Coenegrachts", "@stevencoenegrachts.bsky.social"),
        ("Pierre-Yves Dermagne", "@pydermagne.bsky.social"),
        ("Caroline Désir", "@carodesir.bsky.social"),
        ("François De Smet", "@francoisdesmet.bsky.social"),
        ("Petra De Sutter", "@petradesutter.bsky.social"),
        ("Nawal Farih", "@nawalfarih.bsky.social"),
        ("Raoul Hedebouw", "@raoulhedebouw.bsky.social"),
        ("Christophe Lacroix", "@lacroixch.bsky.social"),
        ("Paul Magnette", "@paulmagnette.bsky.social"),
        ("Sammy Mahdi", "@sammymahdi.bsky.social"),
        ("Dieter vanbesien", "@dietervanbesien.bsky.social"),
        ("Steven Matheï", "@stevenmathei.bsky.social"),
        ("Sofie Merckx", "@sofiemerckx.bsky.social"),
        ("Peter Mertens", "@petermertens.bsky.social"),
        ("Patrick Prévot", "@patrickprevot.bsky.social"),
        ("Sarah Schlitz", "@sarahschlitz.bsky.social"),
        ("Matti Vandemaele", "@mattivandemaele.bsky.social"),
        ("Kjell Vander Elst", "@kjellvanderelst.bsky.social"),
        ("Stefaan Van Hecke", "@stefaanvanhecke.bsky.social"),
        ("Vincent Van Quickenborne", "@vincentvq.bsky.social"),
        ("Anja Vanrobaeys", "@anjavanrobaeys.bsky.social"),
        ("Axel Weydts", "@axelweydts.bsky.social"),
        ("Mathieu Bihet", "@mathieubihet.bsky.social"),
        ("Yves Coppieters", "@yvescoppieters.bsky.social"),
        ("Elisabeth Degryse", "@edegryse.bsky.social"),
        ("Melissa Depraetere", "@melissadepraetere.bsky.social"),
        ("Bart De Wever", "@deweverbart.bsky.social"),
        ("Valérie Glatigny", "@valerieglatigny.bsky.social"),
        ("Vincent Van Peteghem", "@vincentvp.cdenv.be"),
        ("Annelies Verlinden", "@anneliesverlinden.bsky.social")
    ]);

    // Create bsky session.
    let agent = BskyAgent::builder().build().await?;
    let session = agent.login(&username, &password).await?;

    // Create headless Chrome browser.
    let launch_options = LaunchOptionsBuilder::default()
        .args(
            [
                "--disable-dev-shm-usage",
                "--no-sandbox",
                "--disable-blink-features=AutomationControlled",
                "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64)...",
            ]
                .iter()
                .map(|s| OsStr::new(s))
                .collect::<Vec<&OsStr>>(),
        )
        .build()
        .unwrap();
    let browser = Browser::new(launch_options)?;

    // Posts.
    let data_dir = get_data_dir();
    let posts_log_path = get_log_file_path("posts.json");
    let mut posts: Vec<BskyPost> = if posts_log_path.exists() {
        let contents = fs::read_to_string(&posts_log_path)?;
        serde_json::from_str(&contents)?
    } else {
        Vec::new()
    };

    // Data readers.
    let votes_path = data_dir.join("votes.parquet");
    let vote_file = File::open(&votes_path)?;
    let mut vote_reader = ParquetRecordBatchReaderBuilder::try_new(vote_file)?.build()?;
    let questions_path = data_dir.join("questions.parquet");
    let questions_file = File::open(&questions_path)?;
    let mut questions_reader = ParquetRecordBatchReaderBuilder::try_new(questions_file)?.build()?;

    // Collect data.
    let mut vote_count = 0;
    let mut date = String::new();
    let mut meeting_id = String::new();
    for batch in &mut vote_reader {
        let batch = batch?;
        let vote_titles = batch
            .column_by_name("title_nl").unwrap()
            .as_any().downcast_ref::<StringArray>().unwrap();
        let meeting_id_col = batch
            .column_by_name("meeting_id").unwrap()
            .as_any().downcast_ref::<StringArray>().unwrap();
        let date_col = batch
            .column_by_name("date").unwrap()
            .as_any().downcast_ref::<StringArray>().unwrap();

        for i in 0..vote_titles.len() {
            if meeting_id_col.value(i) == target_meeting_id {
                vote_count += 1;

                if date.is_empty() {
                    date = date_col.value(i).to_string();
                    meeting_id = meeting_id_col.value(i).to_string();
                }
            }
        }
    }

    let mut question_count = 0;
    for batch in &mut questions_reader {
        let batch = batch?;
        let question_titles = batch
            .column_by_name("topics_nl").unwrap()
            .as_any().downcast_ref::<StringArray>().unwrap();
        let meeting_id_col = batch
            .column_by_name("meeting_id").unwrap()
            .as_any().downcast_ref::<StringArray>().unwrap();

        question_count += (0..question_titles.len())
            .filter(|&i| meeting_id_col.value(i) == target_meeting_id)
            .count();
    }

    let mut main_post_uri: Option<String> = None;

    // Create or assign main post.
    if main_post_uri.is_none() {
        // Create main post for meeting if it does not exist yet.
        if let Some(existing) = posts.iter().find(|p| p.id == target_meeting_id) {
            main_post_uri = Some(existing.uri.clone());
        } else {
            let date_str = format_dutch_date(date.as_str()).unwrap();
            let main_post_text = format!("📣 Nieuwe vergadering van het Belgische Parlement ({})\n- {} stemmingen\n- {} vragen", date_str, vote_count, question_count);
            let main_post = agent.create_record(RecordData {
                created_at: Datetime::now(),
                text: main_post_text,
                facets: None,
                embed: None,
                entities: None,
                labels: None,
                langs: None,
                reply: None,
                tags: None,
            }).await?;

            let post = BskyPost {
                id: meeting_id.to_string(),
                date: meeting_id.to_string(),
                uri: main_post.uri.clone(),
                replies: vec![],
            };
            posts.push(post);
            fs::write(&posts_log_path, serde_json::to_string_pretty(&posts)?)?;
            main_post_uri = Some(main_post.uri.clone());
        }
    }

    // Re-open file for second pass (votes).
    let vote_file = File::open(&votes_path)?;
    let vote_reader = ParquetRecordBatchReaderBuilder::try_new(vote_file)?.build()?;

    for batch in vote_reader {
        let batch = batch?;
        let vote_titles = batch.column_by_name("title_nl").unwrap().as_any().downcast_ref::<StringArray>().unwrap();
        let yes_col = batch.column_by_name("yes").unwrap().as_any().downcast_ref::<StringArray>().unwrap();
        let no_col = batch.column_by_name("no").unwrap().as_any().downcast_ref::<StringArray>().unwrap();
        let abstain_col = batch.column_by_name("abstain").unwrap().as_any().downcast_ref::<StringArray>().unwrap();
        let session_id_col = batch.column_by_name("session_id").unwrap().as_any().downcast_ref::<StringArray>().unwrap();
        let meeting_id_col = batch.column_by_name("meeting_id").unwrap().as_any().downcast_ref::<StringArray>().unwrap();
        let vote_id_col = batch.column_by_name("vote_id").unwrap().as_any().downcast_ref::<StringArray>().unwrap();

        // Loop through votes.
        for i in 0..vote_titles.len() {
            // Only process votes for the target meeting.
            let meeting_id = meeting_id_col.value(i);
            if meeting_id != target_meeting_id {
                continue;
            }

            // Add vote.
            let raw_vote_title = vote_titles.value(i);
            let vote_title_with_handles = replace_names_with_handles(raw_vote_title, &name_to_handle);
            let vote_hash = hash_text(raw_vote_title);

            if let Some(parent) = posts.iter_mut().find(|p| p.id == target_meeting_id) {
                if parent.replies.iter().any(|r| match r {
                    BskyReply::Vote(v) => v.hash == vote_hash,
                    _ => false,
                }) {
                    continue;
                }

                let yes = yes_col.value(i).parse::<u32>().unwrap_or(0);
                let no = no_col.value(i).parse::<u32>().unwrap_or(0);
                let abstain = abstain_col.value(i).parse::<u32>().unwrap_or(0);

                let session_id = session_id_col.value(i);
                let vote_id = vote_id_col.value(i);

                // Take screenshot of vote and upload.
                let vote_url = format!("https://zijwerkenvooru.pages.dev/nl/sessions/{}/meetings/plenary/{}/votes/{}", session_id, meeting_id, vote_id);
                let ScreenshotResult { png_data, viewport } = take_screenshot_of_element(&browser, &vote_url, "#screenshot-target")?;
                let output = agent
                    .api
                    .com
                    .atproto
                    .repo
                    .upload_blob(png_data.as_bytes().to_vec())
                    .await?;
                let width = viewport.width.round() as u64;
                let height = viewport.height.round() as u64;
                let aspect_ratio = Some(atrium_api::app::bsky::embed::defs::AspectRatioData {
                    width: NonZeroU64::new(width).unwrap_or(NonZeroU64::new(1).unwrap()),
                    height: NonZeroU64::new(height).unwrap_or(NonZeroU64::new(1).unwrap()),
                }.into());
                let image = ImageData {
                    alt: "A screenshot of the vote results.".to_string(),
                    image: output.data.blob,
                    aspect_ratio,

                }.into();
                let embed = Some(Union::Refs(
                    RecordEmbedRefs::AppBskyEmbedImagesMain(Box::new(
                        atrium_api::app::bsky::embed::images::MainData {
                            images: vec![image],
                        }.into(),
                    )),
                ));

                // Create vote post text.
                let result = if yes > (no + abstain) { "aangenomen" } else { "verworpen" };
                let total = yes + no + abstain;
                let bar_len: usize = 14;
                let scale = |count: u32| ((count as f64 / total as f64) * bar_len as f64).round() as usize;
                let yes_blocks = scale(yes);
                let no_blocks = scale(no);
                let abstain_blocks = bar_len.saturating_sub(yes_blocks + no_blocks);

                let bar = format!(
                    "{}{}{}\n{}",
                    "🟩".repeat(yes_blocks),
                    "🟥".repeat(no_blocks),
                    "🟧".repeat(abstain_blocks),
                    result
                );

                let vote_post_text = format!(
                    "🗳️ Stemming\n\"{}\"\n\n{}\n\nDetails: https://zijwerkenvooru.be/nl/sessions/{}/meetings/plenary/{}/votes/{}",
                    vote_title_with_handles,
                    bar,
                    session_id, meeting_id, vote_id
                );
                let truncated_post_text = truncate_to_graphemes(&vote_post_text, 300);
                let rich_text = RichText::new_with_detect_facets(&truncated_post_text).await?;

                let reply_to_uri = if let Some(parent) = posts.iter().find(|p| p.id == target_meeting_id) {
                    if let Some(last_reply) = parent.replies.last() {
                        match last_reply {
                            BskyReply::Vote(v) => Some(v.uri.clone()),
                            BskyReply::Question(q) => Some(q.uri.clone()),
                        }
                    } else {
                        // No replies yet, reply to main post
                        main_post_uri.clone()
                    }
                } else {
                    // No parent found, fallback to main post URI
                    main_post_uri.clone()
                };

                let root_uri = main_post_uri.clone();

                let vote_post_uri = create_post(&agent, rich_text.text, rich_text.facets, root_uri, reply_to_uri, embed).await?;

                let vote_reply = BskyReply::Vote(BskyReplyVote {
                    hash: vote_hash.clone(),
                    topic: vote_title_with_handles.to_string(),
                    uri: vote_post_uri.clone(),
                });

                // Find parent post by meeting id and add the reply
                if let Some(parent) = posts.iter_mut().find(|p| p.id == target_meeting_id) {
                    parent.replies.push(vote_reply);
                }

                // Save updated posts to disk
                fs::write(&posts_log_path, serde_json::to_string_pretty(&posts)?)?;
            }
        }
    }

    // Load summaries
    let summaries_path = data_dir.join("summaries.parquet");
    let hashes = read_string_column(&summaries_path, "input_hash")?;
    let summaries = read_string_column(&summaries_path, "summary")?;
    let summary_map: HashMap<String, String> = hashes.into_iter().zip(summaries.into_iter()).collect();


    // Re-open file for second pass (questions).
    let questions_file = File::open(&questions_path)?;
    let questions_reader = ParquetRecordBatchReaderBuilder::try_new(questions_file)?.build()?;

    for batch in questions_reader {
        let batch = batch?;
        let question_titles = batch.column_by_name("topics_nl").unwrap().as_any().downcast_ref::<StringArray>().unwrap();
        let questioners_col = batch.column_by_name("questioners").unwrap().as_any().downcast_ref::<StringArray>().unwrap();
        let respondents_col = batch.column_by_name("respondents").unwrap().as_any().downcast_ref::<StringArray>().unwrap();
        let session_ids_col = batch.column_by_name("session_id").unwrap().as_any().downcast_ref::<StringArray>().unwrap();
        let meeting_ids_col = batch.column_by_name("meeting_id").unwrap().as_any().downcast_ref::<StringArray>().unwrap();
        let question_ids_col = batch.column_by_name("question_id").unwrap().as_any().downcast_ref::<StringArray>().unwrap();

        // Loop through questions.
        for i in 0..question_titles.len() {
            // Only process questions for the target meeting.
            let meeting_id = meeting_ids_col.value(i);
            if meeting_id != target_meeting_id {
                continue;
            }

            // Get question title.
            let question_title = question_titles.value(i);
            let question_hash = hash_text(question_title);
            let topics: Vec<&str> = question_title.split(';').collect();
            let summary = if topics.len() > 1 {
                summary_map.get(&question_hash).cloned().unwrap_or_else(|| topics.join(", "))
            } else {
                topics[0].to_string()
            };

            if let Some(parent) = posts.iter_mut().find(|p| p.id == target_meeting_id) {
                if parent.replies.iter().any(|r| match r {
                    BskyReply::Question(q) => q.hash == question_hash,
                    _ => false,
                }) {
                    continue;
                }

                let format_name = |name: &str| -> String {
                    match name_to_handle.get(name) {
                        Some(handle) => {
                            format!("{}", handle)
                        }
                        None =>  format!("{}", name)
                    }
                };

                let session_id = session_ids_col.value(i);
                let question_id = question_ids_col.value(i);
                let questioners: Vec<String> = questioners_col.value(i)
                    .split(',')
                    .map(|s| format_name(s.trim())) // <-- trim here!
                    .collect();

                let respondents: Vec<String> = respondents_col.value(i)
                    .split(',')
                    .map(|s| format_name(s.trim()))
                    .collect();

                // Take screenshot of question and upload.
                let vote_url = format!("https://zijwerkenvooru.pages.dev/nl/sessions/{}/meetings/plenary/{}/questions/{}", session_id, meeting_id, question_id);
                let ScreenshotResult { png_data, viewport } = take_screenshot_of_element(&browser, &vote_url, "#screenshot-target")?;
                let output = agent
                    .api
                    .com
                    .atproto
                    .repo
                    .upload_blob(png_data.as_bytes().to_vec())
                    .await?;
                let width = viewport.width.round() as u64;
                let height = viewport.height.round() as u64;
                let aspect_ratio = Some(atrium_api::app::bsky::embed::defs::AspectRatioData {
                    width: NonZeroU64::new(width).unwrap_or(NonZeroU64::new(1).unwrap()),
                    height: NonZeroU64::new(height).unwrap_or(NonZeroU64::new(1).unwrap()),
                }.into());
                let image = ImageData {
                    alt: "A screenshot of the question.".to_string(),
                    image: output.data.blob,
                    aspect_ratio,
                }.into();
                let embed = Some(Union::Refs(
                    RecordEmbedRefs::AppBskyEmbedImagesMain(Box::new(
                        atrium_api::app::bsky::embed::images::MainData {
                            images: vec![image],
                        }.into(),
                    )),
                ));

                // Create question post text.
                let question_post_text = format!(
                    "❓Vraag\n\"{}\"\n\n{}\n\nDetails: https://zijwerkenvooru.be/nl/sessions/{}/meetings/plenary/{}/questions/{}",
                    summary,
                    questioners.join(", "),
                    session_id, meeting_id, question_id
                );
                let truncated_post_text = truncate_to_graphemes(&question_post_text, 300);
                let rich_text = RichText::new_with_detect_facets(&truncated_post_text).await?;

                let reply_to_uri = if let Some(parent) = posts.iter().find(|p| p.id == target_meeting_id) {
                    if let Some(last_reply) = parent.replies.last() {
                        match last_reply {
                            BskyReply::Vote(v) => Some(v.uri.clone()),
                            BskyReply::Question(q) => Some(q.uri.clone()),
                        }
                    } else {
                        // No replies yet, reply to main post
                        main_post_uri.clone()
                    }
                } else {
                    // No parent found, fallback to main post URI
                    main_post_uri.clone()
                };

                let root_uri = main_post_uri.clone();

                let question_post_uri = create_post(&agent, rich_text.text, rich_text.facets, root_uri, reply_to_uri, embed).await?;

                let question_reply = BskyReply::Question(BskyReplyQuestion {
                    hash: question_hash.clone(),
                    topic: summary.to_string(),
                    uri: question_post_uri.clone(),
                    questioners,
                    respondents,
                });

                // Find parent post by meeting id and add the reply
                if let Some(parent) = posts.iter_mut().find(|p| p.id == target_meeting_id) {
                    parent.replies.push(question_reply);
                }

                // Save updated posts to disk
                fs::write(&posts_log_path, serde_json::to_string_pretty(&posts)?)?;
            }
        }
    }

    Ok(())
}

pub struct ScreenshotResult {
    pub png_data: Vec<u8>,
    pub viewport: Viewport
}

pub fn take_screenshot_of_element(browser: &Browser, url: &str, selector: &str) -> anyhow::Result<ScreenshotResult> {
    let tab = browser.new_tab()?;

    let tab = tab.set_bounds(headless_chrome::types::Bounds::Normal {
        left: Some(0),
        top: Some(0),
        width: Some(500.0),
        height: Some(1400.0),
    })?;


    // eprintln!("🌐 Navigating to: {}", url);
    tab.navigate_to(url)?;
    tab.wait_until_navigated()?;

    // Wait for body
    tab.wait_for_element_with_custom_timeout("body", Duration::from_secs(10))
        .map(|_| eprintln!("✅ Page loaded (body detected)"))
        .map_err(|e| {
            eprintln!("❌ Timed out waiting for body: {:?}", e);
            e
        })?;

    // Wait for target element
    let element = tab.wait_for_element_with_custom_timeout(selector, Duration::from_secs(10))
        .map_err(|e| {
            eprintln!("❌ Failed to find {}: {:?}", selector, e);
            e
        })?;

    // eprintln!("✅ Found {}", selector);

    let box_model = element.get_box_model()?;
    let content_box = box_model.content_viewport();

    // eprintln!(
    //     "🖼️ Element viewport: x={}, y={}, width={}, height={}",
    //     content_box.x, content_box.y, content_box.width, content_box.height
    // );

    let adjusted_viewport = Some(Viewport {
        x: content_box.x - 10.0,
        y: content_box.y - 7.0,
        width: content_box.width + 20.0,
        height: content_box.height + 14.0,
        scale: 3.0,
    });

    let png_data = tab.capture_screenshot(headless_chrome::protocol::cdp::Page::CaptureScreenshotFormatOption::Png, None, adjusted_viewport, true)?;

    eprintln!("🌐 Captured screenshot for: {}", url);
    Ok(ScreenshotResult {
        png_data,
        viewport: content_box,
    })
}

pub async fn create_post(agent: &BskyAgent, text: String, facets: Option<Vec<atrium_api::app::bsky::richtext::facet::Main>>, root: Option<String>, reply_to: Option<String>, embed: Option<Union<RecordEmbedRefs>>) -> anyhow::Result<String> {
    let mut record = RecordData {
        text,
        created_at: Datetime::now(),
        reply: None,
        embed,
        langs: None,
        labels: None,
        tags: None,
        facets,
        entities: None,
    };

    // If this is a reply, set up the reply reference
    if let Some(reply_uri) = reply_to.clone() {
     //   println!("GET PARENT POST: {}", reply_uri);
        // First get the post we're replying to
        let parent_post = get_post(agent, &reply_uri).await?;

        record.reply = Some(ReplyRefData {
            root: atrium_api::com::atproto::repo::strong_ref::MainData {
                uri: root.unwrap().clone().try_into()?,
                cid: parent_post.cid.clone(),
            }.into(),
            parent: atrium_api::com::atproto::repo::strong_ref::MainData {
                uri: reply_uri.try_into()?,
                cid: parent_post.cid.clone(),
            }.into(),
        }.into());
    }
    let created = agent.create_record(record).await?;

    match reply_to.clone() {
        Some(_) => { println!("Created reply"); },
        None => { println!("Created post"); }
    }

    Ok(created.uri.clone())

    // match agent.create_record(record).await {
    //     Ok(_) => Ok(record.ur),
    //     Err(e) => Err(anyhow::anyhow!("Failed to create post: {}", e))
    // }
}

pub async fn get_post(agent: &BskyAgent, uri: &str) -> anyhow::Result<atrium_api::types::Object<atrium_api::app::bsky::feed::defs::PostViewData>> {
    let get_posts_result = agent.api.app.bsky.feed.get_posts(
        atrium_api::app::bsky::feed::get_posts::ParametersData {
            uris: vec![uri.to_string()],
        }.into()
    ).await;
    if let Ok(post_data) = get_posts_result {
        return Ok(post_data.data.posts[0].clone());
    } else {
        return Err(anyhow::anyhow!("Failed to get post"));
    }
}

fn replace_names_with_handles(input: &str, map: &HashMap<&str, &str>) -> String {
    let mut out = input.to_string();
    for (name, handle) in map {
        // replace full name with handle
        out = out.replace(name, handle);
    }
    out
}