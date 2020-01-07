//! Module that contains all valid record types for this application.
use chrono::Utc;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
/// Struct representing environmental data recorded from a sensor at a specific timestamp.
pub struct EnvironmentalRecord
{
    /// Timestamp the record was recorded.
    pub timestamp: chrono::DateTime<Utc>,
    /// The name of the sensor that recorded the record.
    pub sensor_name: String,
    /// Temperature value in celsius.
    pub celsius: f64,
    /// Relative humidity value.
    pub humidity: f64,
    /// Pressure value in kPa.
    pub pressure: f64,
    /// Illuminance value in lx.
    pub illuminance: f64,
    /// UVA index.
    pub uva: f64,
    /// UVB index.
    pub uvb: f64,
    /// UV Index:
    pub uv: f64,
}