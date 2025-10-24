use std::fmt::Display;

use serde::{Deserialize, Serialize};
use surrealdb::{engine::remote::ws::Client, Error, RecordId, Surreal};

use crate::models::gtfs::{StopTime, Trip};

pub mod gtfs;

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct CSTime {
    hours: usize,
    minutes: usize,
    seconds: usize
}

impl CSTime {
    pub fn parse_from_str(s: &str) -> CSTime {
        let parts: Vec<&str> = s.split(":").collect();
        CSTime { 
            hours: usize::from_str_radix(parts[0], 10).unwrap(), 
            minutes: usize::from_str_radix(parts[1], 10).unwrap(), 
            seconds: usize::from_str_radix(parts[2], 10).unwrap() 
        }
    }
}

impl Display for CSTime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:02}:{:02}:{:02}", self.hours % 24, self.minutes, self.seconds)
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Connection {
    pub dep_stop: RecordId,
    pub arr_stop: RecordId,
    pub dep_time: CSTime,
    pub arr_time: CSTime,
    pub trip: RecordId
}

impl Connection {
    pub async fn build_connections(db: &Surreal<Client>, trips: Vec<Trip>, stop_times: Vec<StopTime>) -> Result<(), Error> {
        for (index, trip) in trips.iter().enumerate() {
            let mut times: Vec<&StopTime> = stop_times.iter().filter(|x| x.trip_id == trip.trip_id).collect();
            times.sort_by(|a, b| a.stop_sequence.cmp(&b.stop_sequence));
            
            for pair in times.windows(2) {
                let _: Vec<Connection> = db.insert("connection").content(Connection {
                        dep_stop: pair[0].stop_id.clone(),
                        arr_stop: pair[1].stop_id.clone(),
                        dep_time: pair[0].departure_time.clone(),
                        arr_time: pair[1].arrival_time.clone(),
                        trip: pair[0].trip_id.clone(),
                    }).await?;
            }
            print!("Build Connections for {}% of Trips\r", (index*100)/trips.len());
        }
        println!("Build Connections for 100% of Trips");

        Ok(())
    }
}
