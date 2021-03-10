use chrono::DateTime;
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;
use std::collections::HashMap;

#[skip_serializing_none]
#[derive(Debug, Deserialize, Serialize)]
pub struct NativeDataFrame {
    #[serialize_always]
    pub OwnerUserId: Option<f64>,
    #[serialize_always]
    pub PostClosedDate: Option<String>,
    #[serialize_always]
    pub PostCreationDate: Option<String>,
    #[serialize_always]
    pub PostId: Option<f64>,
    #[serialize_always]
    pub ReputationAtPostCreation: Option<f64>,
    #[serialize_always]
    pub BodyMarkdown: Option<String>,
    #[serialize_always]
    pub Tag4: Option<String>,
    #[serialize_always]
    pub Tag1: Option<String>,
    #[serialize_always]
    pub OwnerCreationDate: Option<String>,
    #[serialize_always]
    pub Tag5: Option<String>,
    #[serialize_always]
    pub Tag3: Option<String>,
    #[serialize_always]
    pub OpenStatus: Option<String>,
    #[serialize_always]
    pub Tag2: Option<String>,
    #[serialize_always]
    pub OwnerUndeletedAnswerCountAtPostTime: Option<f64>,
    #[serialize_always]
    pub Title: Option<String>,
    #[serde(skip)]
    pub PostCreationDatetime: Option<DateTime<chrono::FixedOffset>>,
    #[serialize_always]
    pub CountWords: Option<f64>,
    #[serde(skip)]
    pub Wikipedia: Option<WikiDataFrame>,
}

#[skip_serializing_none]
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct WikiDataFrame {
    #[serialize_always]
    pub Language: Option<String>,
    #[serialize_always]
    pub Procedural: Option<f64>,
    #[serialize_always]
    #[serde(rename(serialize = "Object-oriented", deserialize = "Object-oriented"))]
    pub ObjectOriented: Option<f64>,
    #[serialize_always]
    pub Imperative: Option<f64>,
    #[serialize_always]
    pub Functional: Option<f64>,
    #[serialize_always]
    pub Generic: Option<f64>,
    #[serialize_always]
    pub Reflective: Option<f64>,
    #[serialize_always]
    #[serde(rename(serialize = "Event-driven", deserialize = "Event-driven"))]
    pub EventDriven: Option<f64>,
    #[serialize_always]
    #[serde(rename(serialize = "Other paradigm(s)", deserialize = "Other paradigm(s)"))]
    pub OtherParadigm: Option<String>,
    #[serialize_always]
    #[serde(rename(serialize = "Intended use", deserialize = "Intended use"))]
    pub IntendedUse: Option<String>,
    #[serialize_always]
    #[serde(rename(serialize = "Standardized?", deserialize = "Standardized?"))]
    pub Standardized: Option<String>,
}

pub type Record = HashMap<String, String>;

#[derive(Debug, Deserialize, Serialize)]
pub struct GroupBy {
    pub status: String,
    pub ReputationAtPostCreation: f64,
    pub OwnerUndeletedAnswerCountAtPostTime: f64,
    pub Imperative: f64,
    pub ObjectOriented: f64,
    pub Functional: f64,
    pub Procedural: f64,
    pub Generic: f64,
    pub Reflective: f64,
    pub EventDriven: f64,
}

pub fn inspect(path: &str) {
    let mut record: Record = HashMap::new();

    let mut rdr = csv::Reader::from_path(path).unwrap();

    for result in rdr.deserialize() {
        match result {
            Ok(rec) => {
                record = rec;
                break;
            }
            Err(_e) => (),
        };
    }
    // Print Struct
    println!("#[skip_serializing_none]");
    println!("#[derive(Debug, Deserialize, Serialize)]");
    println!("struct lib::DataFrame {{");
    for (key, value) in &record {
        println!("    #[serialize_always]");

        match value.parse::<i64>() {
            Ok(_n) => {
                // println!("    #[serde(deserialize_with = \"empty_string_as_none\")]");
                println!("    {}: Option<i64>,", key);
                continue;
            }
            Err(_e) => (),
        }
        match value.parse::<f64>() {
            Ok(_n) => {
                // println!("    #[serde(deserialize_with = \"empty_string_as_none\")]");
                println!("    {}: Option<f64>,", key);
                continue;
            }
            Err(_e) => (),
        }
        println!("    {}: Option<String>,", key);
    }
    println!("}}");
}
