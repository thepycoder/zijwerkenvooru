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
use std::collections::HashSet;
use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

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
    QuestionTopics,
    DossierTitle,
    QuestionDiscussion,
}

#[tokio::main]
async fn main() {
    // load environment variables
    dotenvy::dotenv().ok();
    let mistral_api_key = std::env::var("MISTRAL_API_TOKEN").expect("Missing MISTRAL_API_TOKEN");
    let client = Client::new();

    let root = PathBuf::from("./web/src/data");
    let commission_questions_path = root.join("commission_questions.parquet");
    let summaries_path = root.join("summaries.parquet");

    // Collect existing input hashes
    let mut existing_hashes = HashSet::new();
    if summaries_path.exists() {
        let file = File::open(&summaries_path).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        let reader = builder.build().unwrap();

        for batch in reader {
            let batch = batch.unwrap();

            let hash_column = batch
                .column_by_name("input_hash")
                .expect("Missing input_hash column")
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("Expected input_hash as StringArray");

            for i in 0..hash_column.len() {
                existing_hashes.insert(hash_column.value(i).to_string());
            }
        }
    }

    let question_titles_task = SummarizationTask {
        task_type: SummarizationTaskType::QuestionTopics,
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

    let question_discussions_task = SummarizationTask {
        task_type: SummarizationTaskType::QuestionDiscussion,
        model_name: "mistral-medium-2508".to_string(),
        prompt: "Je krijgt de volledige discussie (vraag en antwoord) als ruwe tekst. Vat de discussie samen in maximaal 4 zinnen, hoe korter hoe beter. Hou de informatiedensiteit heel hoog, geen onnodige woorden. \
            - Schrijf in het Nederlands. \
            - Benadruk het hoofdonderwerp en de belangrijkste standpunten/antwoorden. \
            - Geen extra uitleg, geen opsommingen, enkel de samenvatting.".to_string(),
        column_name: "discussion".to_string(),
        source_file: root.join("questions.parquet"),
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

    let (question_title_rows, question_title_calls) = run_summarization_task(
        question_titles_task,
        &client,
        &mistral_api_key,
        &existing_hashes,
    )
    .await;

    let (question_discussion_rows, question_discussion_calls) = run_summarization_task(
        question_discussions_task,
        &client,
        &mistral_api_key,
        &existing_hashes,
    )
    .await;

    // let (dossier_title_rows, dossier_title_calls) =
    //     run_summarization_task(dossier_titles_task, &client, api_key, &existing_hashes).await;

    // let (rows_cq_disc, calls_cq_disc) = process_question_discussions(
    //     &client,
    //     &mistral_api_key,
    //     &commission_questions_path,
    //     model_name,
    //     &existing_hashes,
    //     &summaries_path,
    // )
    // .await;
    // let _all_rows = [rows_q, rows_d, rows_q_disc, rows_cq_disc].concat();

    println!(
        "Summarized with a total of {} Mistral API calls",
        question_title_calls + question_discussion_calls
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
    existing_hashes: &HashSet<String>,
) -> (Vec<SummaryRow>, u32) {
    let source_file = File::open(task.source_file).unwrap();
    let source_file_reader = ParquetRecordBatchReaderBuilder::try_new(source_file)
        .unwrap()
        .build()
        .unwrap();

    let mut summary_rows = Vec::new();
    let mut mistral_calls = 0;
    let mut processed_batches = Vec::new();

    for batch_result in source_file_reader {
        let batch = batch_result.expect("Failed to read batch from file");
        processed_batches.push(batch.clone());

        let column = batch
            .column_by_name(task.column_name.as_str())
            .expect("Missing expected column")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Expected a StringArray");

        let progress_bar = ProgressBar::new(column.len() as u64);
        progress_bar.set_style(
            ProgressStyle::default_bar()
                .template("{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos}/{len} ({eta})")
                .unwrap()
                .progress_chars("#>-"),
        );

        for i in 0..column.len() {
            // if mistral_calls >= 3 {
            //     println!("Reached Mistral API call limit.");
            //     pb.finish_with_message(format!("Stopped early after 5 API calls."));
            //     return (summary_rows, mistral_calls);
            // }

            let raw_input = column.value(i);
            let prepared_input = raw_input.to_string();
            let input_hash = hash_text(&prepared_input);
            let should_summarize = match task.task_type {
                SummarizationTaskType::QuestionTopics => {
                    prepared_input.contains(';') && !existing_hashes.contains(&input_hash)
                }
                SummarizationTaskType::DossierTitle => !existing_hashes.contains(&input_hash),
                SummarizationTaskType::QuestionDiscussion => {
                    let trimmed = raw_input.trim();
                    trimmed != "[]" && !trimmed.is_empty() && !existing_hashes.contains(&input_hash)
                }
            };

            if should_summarize {
                if let SummarizationTaskType::QuestionDiscussion = task.task_type {
                    println!(
                        "Sending discussion to Mistral (chars={}, preview=\"{}\")",
                        prepared_input.len(),
                        prepared_input.chars().take(100).collect::<String>()
                    );
                }

                let summary = mistral_complete(
                    client,
                    api_key,
                    &prepared_input,
                    &task.model_name,
                    &task.prompt,
                    &mut mistral_calls,
                )
                .await;

                if let Some(summary) = summary {
                    let row = SummaryRow {
                        input_hash,
                        original: prepared_input.to_string(),
                        summary,
                        model: task.model_name.clone(),
                    };
                    // Persist incrementally by rewriting the summaries file with the new row appended.
                    if let Err(err) = rewrite_summaries_file(&task.target_file, &[row.clone()]) {
                        eprintln!("Failed to write summaries file: {}", err);
                    }
                    summary_rows.push(row);
                }
            }

            progress_bar.inc(1);
            if mistral_calls != 0 {
                println!("Mistral calls: {}", mistral_calls);
            }
        }
        progress_bar.finish_with_message(format!("{} summarization complete!", task.column_name));
    }

    (summary_rows, mistral_calls)
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

    match client
        .post("https://api.mistral.ai/v1/chat/completions")
        .header(CONTENT_TYPE, "application/json")
        .header(ACCEPT, "application/json")
        .header(AUTHORIZATION, format!("Bearer {}", api_key))
        .json(payload)
        .send()
        .await
    {
        Ok(resp) => {
            if resp.status().is_success() {
                let json_resp: ApiResponse = resp.json().await.unwrap();
                *mistral_calls += 1;
                tokio::time::sleep(Duration::from_secs(5)).await;
                Some(strip_markdown(&json_resp.choices[0].message.content))
            } else {
                eprintln!(
                    "HTTP Error: {} - {:?}",
                    resp.status(),
                    resp.text().await.unwrap_or_default()
                );
                None
            }
        }
        Err(err) => {
            eprintln!("Request Failed: {}", err);
            None
        }
    }
}

#[derive(Debug, Clone)]
struct SummaryRow {
    input_hash: String,
    original: String,
    summary: String,
    model: String,
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

fn rewrite_summaries_file(
    summaries_path: &PathBuf,
    new_rows: &[SummaryRow],
) -> Result<(), Box<dyn std::error::Error>> {
    // Gather existing rows from file if present
    let mut input_hashes: Vec<String> = Vec::new();
    let mut originals: Vec<String> = Vec::new();
    let mut summaries: Vec<String> = Vec::new();
    let mut models: Vec<String> = Vec::new();

    if summaries_path.exists() {
        let file = File::open(summaries_path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let reader = builder.build()?;

        for batch in reader {
            let batch = batch?;
            let input_hash_col = batch
                .column_by_name("input_hash")
                .ok_or("Missing input_hash column")?
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or("input_hash must be StringArray")?;
            let original_col = batch
                .column_by_name("original")
                .ok_or("Missing original column")?
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or("original must be StringArray")?;
            let summary_col = batch
                .column_by_name("summary")
                .ok_or("Missing summary column")?
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or("summary must be StringArray")?;
            let model_col = batch
                .column_by_name("model")
                .ok_or("Missing model column")?
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or("model must be StringArray")?;

            for i in 0..batch.num_rows() {
                input_hashes.push(input_hash_col.value(i).to_string());
                originals.push(original_col.value(i).to_string());
                summaries.push(summary_col.value(i).to_string());
                models.push(model_col.value(i).to_string());
            }
        }
    }

    // Append new rows
    for r in new_rows {
        input_hashes.push(r.input_hash.clone());
        originals.push(r.original.clone());
        summaries.push(r.summary.clone());
        models.push(r.model.clone());
    }

    // Build arrays and write a single batch
    let create_column = |name: &str, values: Vec<String>| -> (Arc<Field>, ArrayRef) {
        (
            Arc::new(Field::new(name, DataType::Utf8, false)),
            Arc::new(StringArray::from(values)) as ArrayRef,
        )
    };

    let mut fields = Vec::new();
    let mut columns = Vec::new();

    for (name, values) in [
        ("input_hash", input_hashes),
        ("original", originals),
        ("summary", summaries),
        ("model", models),
    ] {
        let (field, column) = create_column(name, values);
        fields.push(field);
        columns.push(column);
    }

    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(schema.clone(), columns)?;

    let output_file = File::create(summaries_path)?;
    let mut writer = ArrowWriter::try_new(output_file, schema.clone(), None)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}
