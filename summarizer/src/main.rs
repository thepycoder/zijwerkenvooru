use std::collections::HashSet;
use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Array, ArrayRef, StringArray};
use arrow::datatypes::{DataType, Field};
use arrow::{datatypes::Schema, record_batch::RecordBatch};
use parquet::{
    arrow::ArrowWriter, arrow::arrow_reader::ParquetRecordBatchReaderBuilder,
};
use indicatif::{ProgressBar, ProgressStyle};
use reqwest::Client;
use reqwest::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE};
use serde_json::json;
use serde::{Serialize, Deserialize};
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, Copy)]
enum SummarizeTask {
    QuestionTopics,
    DossierTitle,
    QuestionDiscussion,
}

#[tokio::main]
async fn main() {
    let model_name = "mistral";
    let mistral_api_key = std::env::var("MISTRAL_API_TOKEN").expect("Missing MISTRAL_API_TOKEN");
    let client = Client::new();

    let root = PathBuf::from("./web/src/data");
    let questions_path = root.join("questions.parquet");
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

    // Process files (limited for testing inside process_file).
    let (rows_q, calls_q) = process_questions(&client, &mistral_api_key, &questions_path, model_name, &existing_hashes, &summaries_path).await;
    let (rows_d, calls_d) = process_dossiers(&client, &mistral_api_key, &root.join("dossiers.parquet"), model_name, &existing_hashes, &summaries_path).await;
    let (rows_q_disc, calls_q_disc) = process_question_discussions(&client, &mistral_api_key, &questions_path, model_name, &existing_hashes, &summaries_path).await;
    let (rows_cq_disc, calls_cq_disc) = process_question_discussions(&client, &mistral_api_key, &commission_questions_path, model_name, &existing_hashes, &summaries_path).await;
    let _all_rows = [rows_q, rows_d, rows_q_disc, rows_cq_disc].concat();

 //   let existing_batches = [batches_q.clone(), batches_d.clone()].concat();
    let mistral_calls = calls_q + calls_d + calls_q_disc + calls_cq_disc;

    println!("Total Mistral API calls: {}", mistral_calls); // Print total number of API calls
}

async fn process_questions(
    client: &Client,
    api_key: &str,
    path: &PathBuf,
    model_name: &str,
    existing_hashes: &HashSet<String>,
    summaries_path: &PathBuf,
) -> (Vec<SummaryRow>, u32) {
    process_file(
        client,
        api_key,
        path,
        "topics_nl",
        model_name,
        existing_hashes,
        SummarizeTask::QuestionTopics,
        summaries_path,
    )
    .await
}

async fn process_dossiers(
    client: &Client,
    api_key: &str,
    path: &PathBuf,
    model_name: &str,
    existing_hashes: &HashSet<String>,
    summaries_path: &PathBuf,
) -> (Vec<SummaryRow>, u32) {
    process_file(
        client,
        api_key,
        path,
        "title",
        model_name,
        existing_hashes,
        SummarizeTask::DossierTitle,
        summaries_path,
    )
    .await
}

async fn process_question_discussions(
    client: &Client,
    api_key: &str,
    path: &PathBuf,
    model_name: &str,
    existing_hashes: &HashSet<String>,
    summaries_path: &PathBuf,
) -> (Vec<SummaryRow>, u32) {
    process_file(
        client,
        api_key,
        path,
        "discussion",
        model_name,
        existing_hashes,
        SummarizeTask::QuestionDiscussion,
        summaries_path,
    )
    .await
}

async fn process_file(
    client: &Client,
    api_key: &str,
    path: &PathBuf,
    column_name: &str,
    model_name: &str,
    existing_hashes: &HashSet<String>,
    task: SummarizeTask,
    summaries_path: &PathBuf,
) -> (Vec<SummaryRow>, u32) {
    let file = File::open(path).unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let file_reader = builder.build().unwrap();

    let mut summary_rows = Vec::new();
    let mut mistral_calls = 0;
    let mut processed_batches = Vec::new();

    for batch_result in file_reader {
        let batch = batch_result.expect("Failed to read batch from file");
        processed_batches.push(batch.clone());

        let column = batch
            .column_by_name(column_name)
            .expect("Missing expected column")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Expected a StringArray");

        let pb = ProgressBar::new(column.len() as u64);
        pb.set_style(
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
            let should_summarize = match task {
                SummarizeTask::QuestionTopics => prepared_input.contains(';') && !existing_hashes.contains(&input_hash),
                SummarizeTask::DossierTitle => !existing_hashes.contains(&input_hash),
                SummarizeTask::QuestionDiscussion => {
                    let trimmed = raw_input.trim();
                    trimmed != "[]" && !trimmed.is_empty() && !existing_hashes.contains(&input_hash)
                },
            };

            if should_summarize {
                if let SummarizeTask::QuestionDiscussion = task {
                    println!(
                        "Sending discussion to Mistral (chars={}): {}",
                        prepared_input.len(),
                        preview_for_log(&prepared_input, 400)
                    );
                }
                let summary = match task {
                    SummarizeTask::QuestionTopics => summarize_question(client, api_key, &prepared_input, &mut mistral_calls).await,
                    SummarizeTask::DossierTitle => summarize_dossier_title(client, api_key, &prepared_input, &mut mistral_calls).await,
                    SummarizeTask::QuestionDiscussion => summarize_question_discussion(client, api_key, &prepared_input, &mut mistral_calls).await,
                };

                if let Some(summary) = summary {
                    let row = SummaryRow {
                        input_hash,
                        original: prepared_input.to_string(),
                        summary,
                        model: model_name.to_string()
                    };
                    // Persist incrementally by rewriting the summaries file with the new row appended.
                    if let Err(err) = rewrite_summaries_file(summaries_path, &[row.clone()]) {
                        eprintln!("Failed to write summaries file: {}", err);
                    }
                    summary_rows.push(row);
                }
            }

            pb.inc(1);
            println!("Mistral calls: {}", mistral_calls);
        }

        pb.finish_with_message(format!("{} summarization complete!", column_name));
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

async fn summarize_question(client: &Client, api_key: &str, topic: &str, mistral_calls: &mut u32) -> Option<String> {
    let payload = &json!({
        "model": "mistral-large-latest",
        "messages": [
            {
                "role": "system",
                "content": "The assistant will receive a comma-separated list of topics and generate a single, concise topic (no more than 20 words) that encompasses all the given topics. \
                    - The result must match the style of the input topics. \
                    - The result must be in Dutch. \
                    - Do not add explanations, clarifications, or extra words such as 'including' or 'such as'. \
                    - The output should fit naturally within the provided list. \
                    - Only return the summarized topic without any additional text."
            },
            { "role": "user", "content": topic }
        ]
    });

    match client.post("https://api.mistral.ai/v1/chat/completions")
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
                eprintln!("HTTP Error: {} - {:?}", resp.status(), resp.text().await.unwrap_or_default());
                None
            }
        }
        Err(err) => {
            eprintln!("Request Failed: {}", err);
            None
        }
    }
}

async fn summarize_question_discussion(client: &Client, api_key: &str, discussion_text: &str, mistral_calls: &mut u32) -> Option<String> {
    println!(
        "Mistral request (discussion) payload preview (chars={}): {}",
        discussion_text.len(),
        preview_for_log(discussion_text, 400)
    );
    let payload = &json!({
        "model": "mistral-medium-2508",
        "messages": [
            {
                "role": "system",
                "content": "Je krijgt de volledige discussie (vraag en antwoord) als ruwe tekst. Vat de discussie samen in maximaal 4 zinnen, hoe korter hoe beter. Hou de informatiedensiteit heel hoog, geen onnodige woorden. \
                    - Schrijf in het Nederlands. \
                    - Benadruk het hoofdonderwerp en de belangrijkste standpunten/antwoorden. \
                    - Geen extra uitleg, geen opsommingen, enkel de samenvatting."
            },
            { "role": "user", "content": discussion_text }
        ]
    });

    match client.post("https://api.mistral.ai/v1/chat/completions")
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
                let content = json_resp.choices[0].message.content.clone();
                println!(
                    "Mistral response (discussion) preview (chars={}): {}",
                    content.len(),
                    preview_for_log(&content, 300)
                );
                Some(strip_markdown(&content))
            } else {
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                eprintln!(
                    "HTTP Error (discussion): {} - {}",
                    status,
                    preview_for_log(&body, 400)
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

async fn summarize_dossier_title(client: &Client, api_key: &str, topic: &str, mistral_calls: &mut u32) -> Option<String> {
    let payload = &json!({
        "model": "mistral-large-latest",
        "messages": [
            {
                "role": "system",
                "content": "The assistant receives a formal legislative dossier title in Dutch and must generate a concise, summarized version (max. 20 words). \
                    - The summary should clearly convey the core purpose of the law in simple, formal language. \
                    - Focus on the key subject or change the law is addressing, using concise wording like \"Wetsontwerp ter...\" without extra introductory phrases. \
                    - Avoid abbreviations or overly technical jargon. \
                    - Return the summary as a clear and informative sentence without extra text or punctuation. \
                    - The summary should be written in Dutch."
            },
            { "role": "user", "content": topic }
        ]
    });

    match client.post("https://api.mistral.ai/v1/chat/completions")
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
                eprintln!("HTTP Error: {} - {:?}", resp.status(), resp.text().await.unwrap_or_default());
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
    model: String
}

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
    // Remove markdown bold markers while preserving the inner text.
    // We intentionally only strip double markers to avoid affecting italics or lists.
    let without_asterisks = input.replace("**", "");
    let without_underscores = without_asterisks.replace("__", "");
    without_underscores.trim().to_string()
}

fn rewrite_summaries_file(summaries_path: &PathBuf, new_rows: &[SummaryRow]) -> Result<(), Box<dyn std::error::Error>> {
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