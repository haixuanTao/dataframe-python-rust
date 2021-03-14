use chrono::NaiveDateTime;
use serde::{Deserialize, de::Deserializer, Serialize};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct NativeDataFrameRaw {
    pub post_creation_date: String,
    pub reputation_at_post_creation: f64,
    pub body_markdown: String,
    pub tag1: String,
    pub open_status: String,
    pub owner_undeleted_answer_count_at_post_time: f64,
}

impl From<NativeDataFrameRaw> for NativeDataFrame {
    fn from(item: NativeDataFrameRaw) -> Self {
        Self {
            post_creation_date: NaiveDateTime::parse_from_str(&item.post_creation_date, "%m/%d/%Y %H:%M:%S").unwrap(),
            reputation_at_post_creation: item.reputation_at_post_creation,
            count_words: item.body_markdown.split(' ').count() as f64,
            tag1: item.tag1,
            open_status: item.open_status,
            owner_undeleted_answer_count_at_post_time: item.owner_undeleted_answer_count_at_post_time,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct NativeDataFrame {
    #[serde(deserialize_with = "datetime_parser")]
    pub post_creation_date: NaiveDateTime,
    pub reputation_at_post_creation: f64,
    #[serde(deserialize_with = "word_counter", rename = "BodyMarkdown")]
    pub count_words: f64,
    pub tag1: String,
    pub open_status: String,
    pub owner_undeleted_answer_count_at_post_time: f64,
}

fn datetime_parser<'de, D: Deserializer<'de>>(deserializer: D) -> Result<NaiveDateTime, D::Error> {
    let raw: &str = Deserialize::deserialize(deserializer)?;

    NaiveDateTime::parse_from_str(raw, "%m/%d/%Y %H:%M:%S")
        .map_err(|x| serde::de::Error::custom(x.to_string()))
}

fn word_counter<'de, D: Deserializer<'de>>(deserializer: D) -> Result<f64, D::Error> {
    let raw: String = Deserialize::deserialize(deserializer)?;

    Ok(raw.split(' ').count() as f64)
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct WikiDataFrame {
    pub language: String,
    pub procedural: f64,
    #[serde(rename = "Object-oriented")]
    pub object_oriented: f64,
    pub imperative: f64,
    pub functional: f64,
    pub generic: f64,
    pub reflective: f64,
    #[serde(rename = "Event-driven")]
    pub event_driven: f64,
}

#[derive(Debug, Default, Clone, Deserialize, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct GroupBy {
    pub status: String,
    pub reputation_at_post_creation: f64,
    pub owner_undeleted_answer_count_at_post_time: f64,
    pub imperative: f64,
    pub object_oriented: f64,
    pub functional: f64,
    pub procedural: f64,
    pub generic: f64,
    pub reflective: f64,
    pub event_driven: f64,
}
