extern crate postgres;
extern crate chrono;

use chrono::{DateTime, Utc, TimeZone};
use postgres::{Connection, TlsMode};

struct TemperatureRecord {
    id: i64,
    timestamp: chrono::DateTime<Utc>,
    sensor_name: String,
    celsius: f64,
    humidity: f64
}

fn main() {
    let conn = Connection::connect("postgresql://home_user_default:temperature@localhost:5432/home-test-dev", TlsMode::None).unwrap();

    let mut temperature_record = TemperatureRecord {
        id: 4    ,
        timestamp: chrono::Utc::now(),
        sensor_name: "Rust".parse().unwrap(),
        celsius: 1.0,
        humidity: 400.0
    };

    let rows = conn.query("INSERT INTO sensors.records (timestamp, sensor_name) VALUES ($1, $2) RETURNING id",
                 &[&temperature_record.timestamp, &temperature_record.sensor_name]).unwrap();
    let new_id: i64 = rows.get(0).get(0);
    temperature_record.id = new_id;


    conn.execute("INSERT INTO sensors.temperature (record_id, celsius) VALUES ($1, $2)",
                 &[&temperature_record.id, &temperature_record.celsius]).unwrap();
    conn.execute("INSERT INTO sensors.humidity (record_id, humidity) VALUES ($1, $2)",
                 &[&temperature_record.id, &temperature_record.humidity]).unwrap();

    for row in &conn.query("SELECT records.id, records.sensor_name, records.timestamp, \
    Temperature.celsius, humidity.humidity \
    FROM sensors.records \
    INNER JOIN sensors.humidity on humidity.record_id = records.id \
    INNER JOIN sensors.Temperature on Temperature.record_id = records.id", &[]).unwrap() {
        let record = TemperatureRecord {
            id: row.get("id"),
            timestamp: row.get("timestamp"),
            sensor_name: row.get("sensor_name"),
            celsius: row.get("celsius"),
            humidity: row.get("humidity"),
        };

        println!("Found temperature record {} with timestamp {} and values {}, {}", record.id , record.timestamp, record.celsius, record.humidity);
    }

}
