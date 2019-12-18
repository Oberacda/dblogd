use chrono::Utc;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct TemperatureRecord {
    pub id: i64,
    pub timestamp: chrono::DateTime<Utc>,
    pub sensor_name: String,
    pub celsius: f64,
    pub humidity: f64,
}