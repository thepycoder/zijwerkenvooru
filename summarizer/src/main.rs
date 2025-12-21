use arrow::array::{Array, ArrayRef, StringArray};
use arrow::datatypes::{DataType, Field};
use arrow::{datatypes::Schema, record_batch::RecordBatch};
use indicatif::{ProgressBar, ProgressStyle};
use parquet::{arrow::ArrowWriter, arrow::arrow_reader::ParquetRecordBatchReaderBuilder};
use reqwest::Client;
use reqwest::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt::Display;
use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

struct ExistingSummary {
    original: String,
    summary: String,
    model: String,
    meeting_id: Option<String>,
    original_type: Option<String>,
}

/**
 * A summarization task.
 */
struct SummarizationTask {
    task_type: SummarizationTaskType,
    model_name: String,
    prompt: String,
    column_name: String,
    source_file: PathBuf,
    target_file: PathBuf,
}

#[derive(Debug, Clone, Copy)]
enum SummarizationTaskType {
    PlenaryQuestionTopics,
    PlenaryQuestionDiscussion,
    CommissionQuestionTopics,
    CommissionQuestionDiscussion,
    DossierTitle,
}

impl Display for SummarizationTaskType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SummarizationTaskType::PlenaryQuestionTopics => write!(f, "PLENARY_QUESTION_TOPICS"),
            SummarizationTaskType::PlenaryQuestionDiscussion => {
                write!(f, "PLENARY_QUESTION_DISCUSSION")
            }
            SummarizationTaskType::CommissionQuestionTopics => {
                write!(f, "COMMISSION_QUESTION_TOPICS")
            }
            SummarizationTaskType::CommissionQuestionDiscussion => {
                write!(f, "COMMISSION_QUESTION_DISCUSSION")
            }
            SummarizationTaskType::DossierTitle => write!(f, "DOSSIER_TITLE"),
        }
    }
}

/**
 * A row in the summaries file.
 */
#[derive(Debug, Clone)]
struct SummaryRow {
    input_hash: String,
    original: String,
    summary: String,
    model: String,
    meeting_id: Option<String>,
}

const MAX_RETRIES: u32 = 5;
const INITIAL_BACKOFF_MS: u64 = 2_000;
const MAX_BACKOFF_MS: u64 = 60_000;

#[tokio::main]
async fn main() {
    // load environment variables
    dotenvy::dotenv().ok();
    let mistral_api_key = std::env::var("MISTRAL_API_TOKEN").expect("Missing MISTRAL_API_TOKEN");

    // set up http client
    let client = Client::new();

    let root = PathBuf::from("./web/src/data");
    let commission_questions_path = root.join("commission_questions.parquet");
    let summaries_path = root.join("summaries.parquet");

    let mut existing: HashMap<String, ExistingSummary> = HashMap::new();

    if summaries_path.exists() {
        let file = File::open(&summaries_path).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        let reader = builder.build().unwrap();

        for batch in reader {
            let batch = batch.unwrap();

            let input_hash_col = batch
                .column_by_name("input_hash")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();

            let original_col = batch
                .column_by_name("original")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();

            let summary_col = batch
                .column_by_name("summary")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();

            let model_col = batch
                .column_by_name("model")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();

            let meeting_id_col = batch
                .column_by_name("meeting_id")
                .and_then(|c| c.as_any().downcast_ref::<StringArray>());

            let original_type_col = batch
                .column_by_name("type")
                .and_then(|c| c.as_any().downcast_ref::<StringArray>());

            for i in 0..batch.num_rows() {
                existing.insert(
                    input_hash_col.value(i).to_string(),
                    ExistingSummary {
                        original: original_col.value(i).to_string(),
                        summary: summary_col.value(i).to_string(),
                        model: model_col.value(i).to_string(),
                        meeting_id: meeting_id_col.map(|c| c.value(i).to_string()),
                        original_type: original_type_col.map(|c| c.value(i).to_string()),
                    },
                );
            }
        }

        println!("Read summaries.parquet: {} rows total", existing.len());
    }

    let plenary_question_titles_task = SummarizationTask {
        task_type: SummarizationTaskType::PlenaryQuestionTopics,
        model_name: "mistral-large-latest".to_string(),
        prompt: "The assistant will receive a comma-separated list of topics and generate a single, concise topic (no more than 20 words) that encompasses all the given topics. \
            - The result must match the style of the input topics. \
            - The result must be in Dutch. \
            - Do not add explanations, clarifications, or extra words such as 'including' or 'such as'. \
            - The output should fit naturally within the provided list. \
            - Only return the summarized topic without any additional text.".to_string(),
        column_name: "topics_nl".to_string(),
        source_file: root.join("questions.parquet"),
        target_file: root.join("summaries.parquet"),
    };

    let plenary_question_discussions_task = SummarizationTask {
        task_type: SummarizationTaskType::PlenaryQuestionDiscussion,
        model_name: "mistral-medium-2508".to_string(),
        prompt: "Je krijgt de volledige discussie (vraag en antwoord) als ruwe tekst. Vat de discussie samen in maximaal 4 zinnen, hoe korter hoe beter. Hou de informatiedensiteit heel hoog, geen onnodige woorden. \
            - Schrijf in het Nederlands. \
            - Benadruk het hoofdonderwerp en de belangrijkste standpunten/antwoorden. \
            – Formuleer waarderende, kritische of beschuldigende uitspraken expliciet als meningen, kritiek of beweringen van de betrokken spreker (bv. “volgens X”, “X stelt dat”, “X bekritiseert dat”). \
            – Presenteer geen normatieve uitspraken als vaststaande feiten. \
            - Geen extra uitleg, geen opsommingen, enkel de samenvatting.".to_string(),
        column_name: "discussion".to_string(),
        source_file: root.join("questions.parquet"),
        target_file: root.join("summaries.parquet"),
    };

    let commission_question_titles_task = SummarizationTask {
        task_type: SummarizationTaskType::CommissionQuestionTopics,
        model_name: "mistral-large-latest".to_string(),
        prompt: "The assistant will receive a comma-separated list of topics and generate a single, concise topic (no more than 20 words) that encompasses all the given topics. \
            - The result must match the style of the input topics. \
            - The result must be in Dutch. \
            - Do not add explanations, clarifications, or extra words such as 'including' or 'such as'. \
            - The output should fit naturally within the provided list. \
            - Only return the summarized topic without any additional text.".to_string(),
        column_name: "topics_nl".to_string(),
        source_file: root.join("commission_questions.parquet"),
        target_file: root.join("summaries.parquet"),
    };

    let commission_question_discussions_task = SummarizationTask {
        task_type: SummarizationTaskType::PlenaryQuestionDiscussion,
        model_name: "mistral-medium-2508".to_string(),
        prompt: "Je krijgt de volledige discussie (vraag en antwoord) als ruwe tekst. Vat de discussie samen in maximaal 4 zinnen, hoe korter hoe beter. Hou de informatiedensiteit heel hoog, geen onnodige woorden. \
            - Schrijf in het Nederlands. \
            - Benadruk het hoofdonderwerp en de belangrijkste standpunten/antwoorden. \
            – Formuleer waarderende, kritische of beschuldigende uitspraken expliciet als meningen, kritiek of beweringen van de betrokken spreker (bv. “volgens X”, “X stelt dat”, “X bekritiseert dat”). \
            – Presenteer geen normatieve uitspraken als vaststaande feiten. \
            - Geen extra uitleg, geen opsommingen, enkel de samenvatting.".to_string(),
        column_name: "discussion".to_string(),
        source_file: root.join("commission_questions.parquet"),
        target_file: root.join("summaries.parquet"),
    };

    let dossier_titles_task = SummarizationTask {
        task_type: SummarizationTaskType::DossierTitle,
        model_name: "mistral-large-latest".to_string(),
        prompt: "The assistant receives a formal legislative dossier title in Dutch and must generate a concise, summarized version (max. 20 words). \
            - The summary should clearly convey the core purpose of the law in simple, formal language. \
            - Focus on the key subject or change the law is addressing, using concise wording like \"Wetsontwerp ter...\" without extra introductory phrases. \
            - Avoid abbreviations or overly technical jargon. \
            - Return the summary as a clear and informative sentence without extra text or punctuation. \
            - The summary should be written in Dutch.".to_string(),
        column_name: "title".to_string(),
        source_file: root.join("dossiers.parquet"),
        target_file: root.join("summaries.parquet"),
    };

    let plenary_question_titles_calls = run_summarization_task(
        plenary_question_titles_task,
        &client,
        &mistral_api_key,
        &mut existing,
    )
    .await;

    let plenary_question_discussion_calls = run_summarization_task(
        plenary_question_discussions_task,
        &client,
        &mistral_api_key,
        &mut existing,
    )
    .await;

    let commission_question_titles_calls = run_summarization_task(
        commission_question_titles_task,
        &client,
        &mistral_api_key,
        &mut existing,
    )
    .await;

    let commission_question_discussion_calls = run_summarization_task(
        commission_question_discussions_task,
        &client,
        &mistral_api_key,
        &mut existing,
    )
    .await;

    // let (dossier_title_rows, dossier_title_calls) =
    //     run_summarization_task(dossier_titles_task, &client, api_key, &existing_hashes).await;

    rewrite_summaries_file(&summaries_path, &existing).expect("Failed to write summaries");

    println!(
        "Summarized with a total of {} Mistral API calls",
        plenary_question_titles_calls
            + plenary_question_discussion_calls
            + commission_question_titles_calls
            + commission_question_discussion_calls
    );
}

// async fn process_question_discussions(
//     client: &Client,
//     api_key: &str,
//     path: &PathBuf,
//     model_name: &str,
//     existing_hashes: &HashSet<String>,
//     summaries_path: &PathBuf,
// ) -> (Vec<SummaryRow>, u32) {
//     run_summarization_task(
//         client,
//         api_key,
//         path,
//         "discussion",
//         model_name,
//         existing_hashes,
//         SummarizationTaskType::QuestionDiscussion,
//         summaries_path,
//     )
//     .await
// }

/**
 * Run a summarization task.
 */
async fn run_summarization_task(
    task: SummarizationTask,
    client: &Client,
    api_key: &str,
    existing: &mut HashMap<String, ExistingSummary>,
) -> u32 {
    println!("=======================");
    println!("Starting summarization task: {}", task.task_type);
    let source_file = File::open(task.source_file).unwrap();
    let source_file_reader = ParquetRecordBatchReaderBuilder::try_new(source_file)
        .unwrap()
        .build()
        .unwrap();

    let mut mistral_calls = 0;

    for batch_result in source_file_reader {
        let batch = batch_result.expect("Failed to read batch from file");

        let source_column = batch
            .column_by_name(task.column_name.as_str())
            .expect("Missing expected column")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Expected a StringArray");

        let meeting_id_column = batch
            .column_by_name("meeting_id")
            .expect("Missing meeting_id column in source file")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Expected meeting_id as StringArray");

        let progress_bar = ProgressBar::new(source_column.len() as u64);
        progress_bar.set_style(
            ProgressStyle::default_bar()
                .template("{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos}/{len} ({eta})")
                .unwrap()
                .progress_chars("#>-"),
        );

        // Go through each row in the source column
        for i in 0..source_column.len() {
            let meeting_id = meeting_id_column.value(i).to_string();
            let raw_input = source_column.value(i);
            let prepared_input = raw_input.to_string();
            let input_hash = hash_text(&prepared_input);
            let should_summarize = match task.task_type {
                SummarizationTaskType::PlenaryQuestionTopics => {
                    prepared_input.contains(';') && !existing.contains_key(&input_hash)
                }
                SummarizationTaskType::PlenaryQuestionDiscussion => {
                    let trimmed = raw_input.trim();
                    trimmed != "[]" && !trimmed.is_empty() && !existing.contains_key(&input_hash)
                }
                SummarizationTaskType::CommissionQuestionTopics => {
                    prepared_input.contains(';') && !existing.contains_key(&input_hash)
                }
                SummarizationTaskType::CommissionQuestionDiscussion => {
                    let trimmed = raw_input.trim();
                    trimmed != "[]" && !trimmed.is_empty() && !existing.contains_key(&input_hash)
                }
                SummarizationTaskType::DossierTitle => !existing.contains_key(&input_hash),
            };

            if should_summarize {
                // Debug print discussion preview when summarizing
                if let SummarizationTaskType::PlenaryQuestionDiscussion = task.task_type {
                    println!(
                        "Sending discussion to Mistral (chars={}, preview=\"{}\")",
                        prepared_input.len(),
                        prepared_input.chars().take(100).collect::<String>()
                    );
                }

                if let Some(summary) = mistral_complete(
                    client,
                    api_key,
                    &prepared_input,
                    &task.model_name,
                    &task.prompt,
                    &mut mistral_calls,
                )
                .await
                {
                    if !existing.contains_key(&input_hash) {
                        existing.insert(
                            input_hash.clone(),
                            ExistingSummary {
                                original: prepared_input.clone(),
                                summary,
                                model: task.model_name.clone(),
                                meeting_id: Some(meeting_id),
                                original_type: Some(task.task_type.to_string()),
                            },
                        );
                    }
                }
            } else if let Some(e) = existing.get_mut(&input_hash) {
                e.meeting_id = Some(meeting_id);
                e.original_type = Some(task.task_type.to_string());
            }

            progress_bar.inc(1);
            if mistral_calls != 0 {
                println!("Mistral calls: {}", mistral_calls);
            }
        }
        progress_bar.finish_with_message(format!("{} summarization complete!", task.column_name));
    }

    println!(
        "Completed summarization task with {} Mistral API Calls",
        mistral_calls
    );
    println!("=======================");

    mistral_calls
}

#[derive(Serialize, Deserialize, Debug)]
struct Message {
    role: String,
    content: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct Choice {
    message: Message,
}

#[derive(Serialize, Deserialize, Debug)]
struct ApiResponse {
    choices: Vec<Choice>,
}

async fn mistral_complete(
    client: &Client,
    api_key: &str,
    content: &str,
    model: &str,
    prompt: &str,
    mistral_calls: &mut u32,
) -> Option<String> {
    let payload = &json!({
        "model": model,
        "messages": [
            {
                "role": "system",
                "content": prompt,
            },
            { "role": "user", "content": content }
        ]
    });

    let mut attempt: u32 = 0;
    let mut backoff_ms = INITIAL_BACKOFF_MS;

    loop {
        attempt += 1;

        let response = client
            .post("https://api.mistral.ai/v1/chat/completions")
            .header(CONTENT_TYPE, "application/json")
            .header(ACCEPT, "application/json")
            .header(AUTHORIZATION, format!("Bearer {}", api_key))
            .json(&payload)
            .send()
            .await;

        match response {
            Ok(resp) if resp.status().is_success() => {
                let json_resp: ApiResponse = resp.json().await.unwrap();
                *mistral_calls += 1;

                // Optional global throttle
                tokio::time::sleep(Duration::from_secs(5)).await;

                return Some(strip_markdown(&json_resp.choices[0].message.content));
            }

            Ok(resp) if resp.status().as_u16() == 429 || resp.status().is_server_error() => {
                let status = resp.status(); // copy StatusCode (cheap, Copy)
                let body = resp.text().await.unwrap_or_default();

                if attempt >= MAX_RETRIES {
                    eprintln!(
                        "Mistral retry failed after {} attempts. Last error ({}): {}",
                        attempt, status, body
                    );
                    return None;
                }

                eprintln!(
                    "Mistral returned {} (attempt {}/{}). Retrying in {} ms...",
                    status, attempt, MAX_RETRIES, backoff_ms
                );

                tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                backoff_ms = (backoff_ms * 2).min(MAX_BACKOFF_MS);
            }

            Ok(resp) => {
                eprintln!(
                    "Mistral request failed with status {}: {}",
                    resp.status(),
                    resp.text().await.unwrap_or_default()
                );
                return None;
            }

            Err(err) => {
                if attempt >= MAX_RETRIES {
                    eprintln!("Network error after {} attempts: {}", attempt, err);
                    return None;
                }

                eprintln!(
                    "Network error (attempt {}/{}): {}. Retrying in {} ms...",
                    attempt, MAX_RETRIES, err, backoff_ms
                );

                tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                backoff_ms = (backoff_ms * 2).min(MAX_BACKOFF_MS);
            }
        }
    }
}

/// Returns a preview of the given string of up to `limit` bytes, appending a truncated
/// message if the string exceeds the limit. This is useful for debug logging purposes
/// to avoid printing very long strings in full (such as LLM input payloads).
///
/// Ensures the slice ends on a valid UTF-8 boundary.
///
/// # Example
/// ```
/// let text = "Dit is een hele lange inputtekst die getoond moet worden...";
/// println!("{}", preview_for_log(text, 20)); // Shows: "Dit is een hele lange… [truncated, 58 chars total]"
/// ```
fn preview_for_log(s: &str, limit: usize) -> String {
    if s.len() <= limit {
        s.to_string()
    } else {
        // Ensure we cut at a valid UTF-8 char boundary
        let safe_end = if s.is_char_boundary(limit) {
            limit
        } else {
            let mut idx = limit;
            while idx > 0 && !s.is_char_boundary(idx) {
                idx -= 1;
            }
            idx
        };
        let prefix = &s[..safe_end];
        format!("{}… [truncated, {} chars total]", prefix, s.len())
    }
}

fn hash_text(input: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(input.as_bytes());
    format!("{:x}", hasher.finalize())
}

fn strip_markdown(input: &str) -> String {
    // Remove markdown bold (**) and italics (*) and underline (__) and (_) markers while preserving the inner text.
    let mut s = input.replace("**", "");
    s = s.replace("__", "");
    s = s.replace("*", "");
    s = s.replace("_", "");
    s.trim().to_string()
}

/**
 * Rewrite the summaries file.
 */
fn rewrite_summaries_file(
    summaries_path: &PathBuf,
    existing: &HashMap<String, ExistingSummary>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut input_hashes = Vec::new();
    let mut originals = Vec::new();
    let mut summaries = Vec::new();
    let mut models = Vec::new();
    let mut meeting_ids = Vec::new();
    let mut types = Vec::new();

    for (hash, row) in existing {
        input_hashes.push(hash.clone());
        originals.push(row.original.clone());
        summaries.push(row.summary.clone());
        models.push(row.model.clone());
        meeting_ids.push(row.meeting_id.clone().unwrap_or_default());
        types.push(row.original_type.clone().unwrap_or_default());
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("input_hash", DataType::Utf8, false),
        Field::new("original", DataType::Utf8, false),
        Field::new("summary", DataType::Utf8, false),
        Field::new("model", DataType::Utf8, false),
        Field::new("meeting_id", DataType::Utf8, true),
        Field::new("type", DataType::Utf8, true),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(input_hashes)),
            Arc::new(StringArray::from(originals)),
            Arc::new(StringArray::from(summaries)),
            Arc::new(StringArray::from(models)),
            Arc::new(StringArray::from(meeting_ids)),
            Arc::new(StringArray::from(types)),
        ],
    )?;

    let file = File::create(summaries_path)?;
    let mut writer = ArrowWriter::try_new(file, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;

    Ok(())
}
