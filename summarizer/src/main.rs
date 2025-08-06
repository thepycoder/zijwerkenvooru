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

#[tokio::main]
async fn main() {
    let model_name = "mistral";
    let mistral_api_key = std::env::var("MISTRAL_API_TOKEN").expect("Missing MISTRAL_API_TOKEN");
    let client = Client::new();

    let root = PathBuf::from("./web/src/data");
    let questions_path = root.join("questions.parquet");
    let summaries_path = root.join("summaries.parquet");

    // Collect existing input hashes
    let mut existing_hashes = HashSet::new();
    let mut existing_batches = Vec::new();
    if summaries_path.exists() {
        let file = File::open(&summaries_path).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        let reader = builder.build().unwrap();

        for batch in reader {
            let batch = batch.unwrap();
            existing_batches.push(batch.clone());

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

    // Process files.
    let (rows_q, calls_q) = process_questions(&client, &mistral_api_key, &questions_path, model_name, &existing_hashes).await;
    let (rows_d, calls_d) = process_dossiers(&client, &mistral_api_key, &root.join("dossiers.parquet"), model_name, &existing_hashes).await;
    let all_rows = [rows_q, rows_d].concat();

    let input_hashes: Vec<String> = all_rows.iter().map(|r| r.input_hash.clone()).collect();
    let originals: Vec<String> = all_rows.iter().map(|r| r.original.clone()).collect();
    let summaries: Vec<String> = all_rows.iter().map(|r| r.summary.clone()).collect();
    let models: Vec<String> = all_rows.iter().map(|r| r.model.clone()).collect();

 //   let existing_batches = [batches_q.clone(), batches_d.clone()].concat();
    let mistral_calls = calls_q + calls_d;

    // If we generated new summaries, write them along with previous ones
    if !all_rows.is_empty() {
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
        let new_batch = RecordBatch::try_new(schema.clone(), columns).unwrap();

        // ✅ Only include previous summaries (not input data!)
        let mut all_batches = existing_batches.clone();
        all_batches.push(new_batch);

        let output_file = File::create(&summaries_path).expect("Failed to create output file");
        let mut writer = ArrowWriter::try_new(output_file, schema.clone(), None)
            .expect("Failed to create writer");

        for batch in all_batches {
            writer.write(&batch).expect("Failed to write batch");
        }
        writer.close().expect("Failed to close writer");
    }

    println!("Total Mistral API calls: {}", mistral_calls); // Print total number of API calls
}

async fn process_questions(
    client: &Client,
    api_key: &str,
    path: &PathBuf,
    model_name: &str,
    existing_hashes: &HashSet<String>,
) -> (Vec<SummaryRow>, u32) {
    process_file(client, api_key, path, "topics_nl", model_name, existing_hashes, true).await
}

async fn process_dossiers(
    client: &Client,
    api_key: &str,
    path: &PathBuf,
    model_name: &str,
    existing_hashes: &HashSet<String>,
) -> (Vec<SummaryRow>, u32) {
    process_file(client, api_key, path, "title", model_name, existing_hashes, false).await
}


async fn process_file(
    client: &Client,
    api_key: &str,
    path: &PathBuf,
    column_name: &str,
    model_name: &str,
    existing_hashes: &HashSet<String>,
    is_question: bool,
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


            let input = column.value(i);
            let input_hash = hash_text(input);
            let should_summarize = (!is_question || input.contains(';')) && !existing_hashes.contains(&input_hash);

            if should_summarize {
                let summary = match is_question {
                    true => summarize_question(client, api_key, input, &mut mistral_calls).await,
                    false => summarize_dossier_title(client, api_key, input, &mut mistral_calls).await,
                };

                if let Some(summary) = summary {
                    summary_rows.push(SummaryRow {
                        input_hash,
                        original: input.to_string(),
                        summary,
                        model: model_name.to_string()
                    });
                }
            }

            pb.inc(1);
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
                Some(json_resp.choices[0].message.content.clone())
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
                Some(json_resp.choices[0].message.content.clone())
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



fn hash_text(input: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(input.as_bytes());
    format!("{:x}", hasher.finalize())
}