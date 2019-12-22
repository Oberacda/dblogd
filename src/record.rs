//! Module that contains all valid record types for this application.
use chrono::Utc;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
/// Struct representing a temperature and humidity record.
pub struct TemperatureRecord
{
    /// Timestamp the record was recorded.
    pub timestamp: chrono::DateTime<Utc>,
    /// The name of the sensor that recorded the record.
    pub sensor_name: String,
    /// Temperature value in celsius.
    pub celsius: f64,
    /// Relative humidity value.
    pub humidity: f64,
}