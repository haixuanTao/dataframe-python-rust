use polars::datatypes::{DataType, Field, Schema};

pub fn get_schema() -> Schema {
    Schema::new(vec![
        // Field::new("Index", DataType::Int32),
        Field::new("PostId", DataType::Int32),
        Field::new("PostCreationDate", DataType::Utf8),
        Field::new("OwnerUserId", DataType::Int32),
        Field::new("OwnerCreationDate", DataType::Utf8),
        Field::new("ReputationAtPostCreation", DataType::Int32),
        Field::new("OwnerUndeletedAnswerCountAtPostTime", DataType::Int32),
        Field::new("Title", DataType::Utf8),
        Field::new("BodyMarkdown", DataType::Utf8),
        Field::new("Tag1", DataType::Utf8),
        Field::new("Tag2", DataType::Utf8),
        Field::new("Tag3", DataType::Utf8),
        Field::new("Tag4", DataType::Utf8),
        Field::new("Tag5", DataType::Utf8),
        Field::new("PostClosedDate", DataType::Utf8),
        Field::new("OpenStatus", DataType::Utf8),
    ])
}
