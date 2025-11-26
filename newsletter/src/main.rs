use arrow::array::Int32Array;
use arrow_array::{Array, RecordBatch, StringArray};
use chrono::Datelike;
use chrono::NaiveDate;
use chrono::NaiveTime;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::collections::HashMap;
use std::error::Error;
use std::fs;
use std::fs::File;
use std::path::{Path, PathBuf};

// Translate time of day to Dutch
fn translate_time_of_day(s: &str) -> String {
    match s {
        "morning" | "voormiddag" => "Voormiddag".to_string(),
        "afternoon" | "namiddag" => "Namiddag".to_string(),
        "evening" | "avond" => "Avond".to_string(),
        _ => s.to_string(),
    }
}

// Parse time string (either "HH:MM" or "HHhMM")
fn parse_time(s: &str) -> Result<NaiveTime, chrono::ParseError> {
    NaiveTime::parse_from_str(s, "%H:%M").or_else(|_| NaiveTime::parse_from_str(s, "%Hh%M"))
}

// Paths
fn data_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("web/src/data")
}

fn posts_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("posts")
}

// Read Parquet batches
fn read_batches(path: &Path) -> anyhow::Result<Vec<RecordBatch>> {
    let file = File::open(path)?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
    let mut batches = Vec::new();
    for batch in reader {
        batches.push(batch?);
    }
    Ok(batches)
}

fn parse_names_opt(arr: &StringArray, i: usize) -> Vec<String> {
    if arr.is_null(i) {
        vec![]
    } else {
        arr.value(i)
            .split(',')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .collect()
    }
}

// Get a string column from a RecordBatch
fn get_string_col<'a>(batch: &'a RecordBatch, name: &str) -> &'a StringArray {
    batch
        .column_by_name(name)
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
}

fn get_i32_col<'a>(batch: &'a RecordBatch, name: &str) -> &'a Int32Array {
    batch
        .column_by_name(name)
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap()
}

#[derive(Debug)]
struct QuestionInfo {
    topic: String,
    questioners: Vec<String>, // array of full names
    respondents: Vec<String>, // array of full names
}
#[derive(Debug)]
struct MemberInfo {
    party: String,
    active: bool,
}

#[derive(Debug)]
struct VoteInfo {
    title: String,
    yes_votes: Vec<String>,
    no_votes: Vec<String>,
    abstain_votes: Vec<String>,
    yes: usize,
    no: usize,
    abstain: usize,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let data_dir = data_dir();
    let posts_dir = posts_dir();

    fs::create_dir_all(&posts_dir)?;

    // Read members
    let members_path = data_dir.join("members.parquet");
    let member_batches = read_batches(&members_path)?;
    let mut member_infos: HashMap<String, MemberInfo> = HashMap::new();
    for batch in member_batches {
        let first_name_col = get_string_col(&batch, "first_name");
        let last_name_col = get_string_col(&batch, "last_name");
        let party_col = get_string_col(&batch, "party");
        let active_col = get_string_col(&batch, "active"); // assume "true"/"false" as string

        for i in 0..first_name_col.len() {
            let name = format!("{} {}", first_name_col.value(i), last_name_col.value(i));
            let party = party_col.value(i).to_string();
            let active = active_col.value(i).to_lowercase() == "true";

            member_infos.insert(name, MemberInfo { party, active });
        }
    }

    // Read votes
    // --- Read votes ---
    let votes_path = data_dir.join("votes.parquet");
    let vote_batches = read_batches(&votes_path)?;
    let mut meeting_votes: HashMap<String, Vec<VoteInfo>> = HashMap::new();

    for batch in vote_batches {
        let meeting_id_col = get_string_col(&batch, "meeting_id");
        let title_col = get_string_col(&batch, "title_nl");
        let yes_count_col = get_string_col(&batch, "yes");
        let no_count_col = get_string_col(&batch, "no");
        let abstain_count_col = get_string_col(&batch, "abstain");

        let yes_voters_col = get_string_col(&batch, "members_yes");
        let no_voterscol = get_string_col(&batch, "members_no");
        let abstain_voters_col = get_string_col(&batch, "members_abstain");

        for i in 0..meeting_id_col.len() {
            let meeting_id = meeting_id_col.value(i).to_string();

            let yes_count: usize = yes_count_col.value(i).parse().unwrap_or(0);
            let no_count: usize = no_count_col.value(i).parse().unwrap_or(0);
            let abstain_count: usize = abstain_count_col.value(i).parse().unwrap_or(0);

            // Split comma-separated names, trim, filter out empty strings
            let yes_voters: Vec<String> = yes_voters_col
                .value(i)
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();

            let no_voters: Vec<String> = no_voterscol
                .value(i)
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();

            let abstain_voters: Vec<String> = abstain_voters_col
                .value(i)
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();

            meeting_votes
                .entry(meeting_id)
                .or_insert_with(Vec::new)
                .push(VoteInfo {
                    title: title_col.value(i).to_string(),
                    yes_votes: yes_voters,
                    no_votes: no_voters,
                    abstain_votes: abstain_voters,
                    yes: yes_count,
                    no: no_count,
                    abstain: abstain_count,
                });
        }
    }

    // Compute absentees
    let active_members: Vec<String> = member_infos
        .iter()
        .filter(|(_, info)| info.active)
        .map(|(name, _)| name.clone())
        .collect();

    // --- Read meetings ---
    let meetings_path = data_dir.join("meetings.parquet");
    let meeting_batches = read_batches(&meetings_path)?;

    #[derive(Debug)]
    struct MeetingInfo {
        session_id: String,
        meeting_id: String,
        date: String,
        time_of_day: String,
        start_time: String,
        end_time: String,
    }

    let mut meetings: HashMap<String, MeetingInfo> = HashMap::new();

    for batch in meeting_batches {
        let session_id = get_string_col(&batch, "session_id");
        let meeting_id = get_string_col(&batch, "meeting_id");
        let date = get_string_col(&batch, "date");
        let time_of_day = get_string_col(&batch, "time_of_day");
        let start_time = get_string_col(&batch, "start_time");
        let end_time = get_string_col(&batch, "end_time");

        for i in 0..meeting_id.len() {
            let id = meeting_id.value(i).to_string();
            meetings.insert(
                id.clone(),
                MeetingInfo {
                    session_id: session_id.value(i).to_string(),
                    meeting_id: id,
                    date: date.value(i).to_string(),
                    time_of_day: time_of_day.value(i).to_string(),
                    start_time: start_time.value(i).to_string(),
                    end_time: end_time.value(i).to_string(),
                },
            );
        }
    }

    // --- Read questions ---
    let questions_path = data_dir.join("questions.parquet");
    let question_batches = read_batches(&questions_path)?;

    let mut meeting_questions: HashMap<String, Vec<QuestionInfo>> = HashMap::new();

    for batch in question_batches {
        let q_topics = get_string_col(&batch, "topics_nl");
        let meeting_id = get_string_col(&batch, "meeting_id");
        let questioners_col = get_string_col(&batch, "questioners"); // comma-separated full names
        let respondents_col = get_string_col(&batch, "respondents"); // comma-separated full names

        for i in 0..meeting_id.len() {
            let m = meeting_id.value(i).to_string();

            // Only take first topic (split by ';')
            let full_topic = q_topics.value(i);
            let first_topic = full_topic
                .split(';')
                .next()
                .unwrap_or(full_topic)
                .trim()
                .to_string();

            // Split comma-separated arrays
            let questioners: Vec<String> = questioners_col
                .value(i)
                .split(',')
                .map(|s| {
                    let name = s.trim();
                    let party = member_infos
                        .get(name)
                        .map(|info| format!(" ({})", info.party))
                        .unwrap_or_default();
                    format!("{}{}", name, party)
                })
                .collect();

            let respondents: Vec<String> = respondents_col
                .value(i)
                .split(',')
                .map(|s| {
                    let name = s.trim();
                    let party = member_infos
                        .get(name)
                        .map(|info| format!(" ({})", info.party))
                        .unwrap_or_default();
                    format!("{}{}", name, party)
                })
                .collect();

            meeting_questions
                .entry(m)
                .or_insert_with(Vec::new)
                .push(QuestionInfo {
                    topic: first_topic,
                    questioners,
                    respondents,
                });
        }
    }

    // --- Generate Markdown files ---
    for (meeting_id, info) in &meetings {
        let qs = meeting_questions
            .get(meeting_id)
            .map(|v| v.as_slice())
            .unwrap_or(&[]);
        let votes = meeting_votes
            .get(meeting_id)
            .map(|v| v.as_slice())
            .unwrap_or(&[]);

        let mut present_members: Vec<String> = Vec::new();
        for vote in votes {
            present_members.extend(vote.yes_votes.iter().cloned());
            present_members.extend(vote.no_votes.iter().cloned());
            present_members.extend(vote.abstain_votes.iter().cloned());
        }
        present_members.sort();
        present_members.dedup();

        let mut absentees: Vec<String> = active_members
            .iter()
            .filter(|m| !present_members.contains(m))
            .cloned()
            .collect();
        let empty = "".to_string();

        absentees.sort_by(|a, b| {
            let party_a = member_infos.get(a).map(|m| &m.party).unwrap_or(&empty);
            let party_b = member_infos.get(b).map(|m| &m.party).unwrap_or(&empty);
            party_a.cmp(party_b)
        });

        let dutch_time_of_day = translate_time_of_day(&info.time_of_day);

        let start = parse_time(&info.start_time).unwrap();
        let end = parse_time(&info.end_time).unwrap();
        let duration_minutes = (end - start).num_minutes();

        // Generate markdown.
        let mut md = String::new();
        // TITLE
        let date = NaiveDate::parse_from_str(&info.date, "%Y-%m-%d")?;
        let dutch_month = match date.month() {
            1 => "januari",
            2 => "februari",
            3 => "maart",
            4 => "april",
            5 => "mei",
            6 => "juni",
            7 => "juli",
            8 => "augustus",
            9 => "september",
            10 => "oktober",
            11 => "november",
            12 => "december",
            _ => "onbekend",
        };
        let formatted_date2 = format!("{} {} {}", date.day(), dutch_month, date.year());

        md.push_str(&format!(
            "Verslag van de plenaire vergadering van {}.\n\nHet volledig verslag (met vragen/antwoorden) is te vinden op https://zijwerkenvooru.be/sessions/56/meetings/plenary/{}/.\n\n\n",
            formatted_date2, meeting_id
        ));

        // TIME AND DATE
        md.push_str(&format!(
            "📅 **Datum:** {} ({})\n\n",
            info.date, dutch_time_of_day
        ));
        md.push_str(&format!(
            "⏱ **Duur:** {}–{} ({} minuten)\n\n",
            info.start_time, info.end_time, duration_minutes
        ));

        // Add absentees
        md.push_str("**Afwezigen**\n\n");
        md.push_str("Dit is een schatting op basis van deelname aan de gevonden stemmingen. De Kamer publiceert geen aanwezigheden.\n\n");
        if absentees.is_empty() {
            md.push_str("_Geen afwezigen_\n\n");
        } else {
            for a in &absentees {
                let party = member_infos
                    .get(a)
                    .map(|info| format!(" ({})", info.party))
                    .unwrap_or_default();
                let slug = a.to_lowercase().replace(' ', "-");
                md.push_str(&format!("- {}{}\n", a, party));
            }
            md.push_str("\n");
        }

        // Questions
        md.push_str(&format!("❓Vragen ({} totaal)\n\n", qs.len()));

        for (idx, q) in qs.iter().enumerate() {
            md.push_str(&format!("**{}. {}**\n\n", idx + 1, q.topic));

            md.push_str("Gesteld door:\n\n");
            for a in &q.questioners {
                let name_only = a.split(" (").next().unwrap_or(a);
                let slug = name_only.to_lowercase().replace(' ', "-");
                md.push_str(&format!("- *{}*\n\n", a));
            }
            md.push_str("\nAan: ");
            for r in &q.respondents {
                let name_only = r.split(" (").next().unwrap_or(r);
                let slug = name_only.to_lowercase().replace(' ', "-");
                md.push_str(&format!("*{}* ", r));
            }
            md.push_str("\n\n\n");
        }

        // Votes
        md.push_str(&format!("🗳️ Naamstemmingen ({} totaal)\n\n", votes.len()));

        if votes.is_empty() {
            md.push_str("_Geen naamstemmingen_\n\n");
        } else {
            for (idx, vote) in votes.iter().enumerate() {
                let yes_count = vote.yes;
                let no_count = vote.no;
                let abstain_count = vote.abstain;
                let total = yes_count + no_count + abstain_count;

                md.push_str(&format!(
                    "**{}. {}**\n\n",
                    idx + 1,
                    vote.title.replace("- ", "")
                ));
                md.push_str(&format!(
                    "Ja ({}), Nee ({}), Onthouding ({})\n\n",
                    yes_count, no_count, abstain_count
                ));

                if total > 0 {
                    // Number of squares to display
                    let bar_width: usize = 25;

                    // Calculate proportional squares
                    let yes_squares =
                        ((yes_count as f64 / total as f64) * bar_width as f64).round() as usize;
                    let no_squares =
                        ((no_count as f64 / total as f64) * bar_width as f64).round() as usize;
                    let abstain_squares = bar_width.saturating_sub(yes_squares + no_squares);

                    // Build the bar
                    let bar = format!(
                        "{}{}{}",
                        "🟩".repeat(yes_squares),
                        "🟥".repeat(no_squares),
                        "🟨".repeat(abstain_squares)
                    );

                    md.push_str(&format!("{}\n\n", bar));
                } else {
                    md.push_str("\n");
                }
            }
        }

        let file_path = posts_dir.join(format!("{}.md", meeting_id));
        fs::write(&file_path, md)?;
        println!("Generated {}", file_path.display());
    }

    println!("Done.");
    Ok(())
}
