use chrono::NaiveDate;
use serde::{Deserialize, Serialize};
use surrealdb::RecordId;

use crate::models::CSTime;

#[derive(Debug, Serialize, Deserialize)]
pub enum ExceptionType {
    ADDED,
    REMOVED
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CalendarDate {
    service_id: String,
    date: NaiveDate,
    excpetion_type: ExceptionType
}

impl CalendarDate {
    pub fn new(source: &str) -> CalendarDate {
        let parts: Vec<&str> = source.split(",").collect();
        CalendarDate { 
            service_id: parts[0].to_string(), 
            date: NaiveDate::parse_from_str(parts[1], "%Y%m%d").unwrap(), 
            excpetion_type: match parts[2] {
                "1" => ExceptionType::ADDED,
                "2" => ExceptionType::REMOVED,
                _=> panic!("Invalid excpetion_type!")
            } 
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub enum RouteType {
    TRAM,
    METRO,
    RAIL,
    BUS,
    FERRY,
    CABLETRAM,
    AERIALLIFT,
    FUNICULAR,
    TROLLEYBUS,
    MONORAIL
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Route {
    pub route_id: RecordId,
    agency_id: String,
    route_short_name: String,
    route_long_name: String,
    pub route_type: RouteType
}

impl Route {
    pub fn new(source: &str) -> Route {
        let mut parts = Vec::new();
        let mut start = 0;
        let mut in_quotes = false;

        for (i, c) in source.char_indices() {
            match c {
                '"' => in_quotes = !in_quotes,
                ',' if !in_quotes => {
                    parts.push(source[start..i].trim());
                    start = i + 1;
                }
                _ => {}
            }
        }

        parts.push(source[start..].trim());

        Route { 
            route_id: RecordId::from(("route", parts[0])), 
            agency_id: parts[1].to_string(), 
            route_short_name: parts[2].to_string(), 
            route_long_name: parts[3].to_string(), 
            route_type: match parts[5] {
                "0" => RouteType::TRAM,
                "1" => RouteType::METRO,
                "2" => RouteType::RAIL,
                "3" => RouteType::BUS,
                "4" => RouteType::FERRY,
                "5" => RouteType::CABLETRAM,
                "6" => RouteType::AERIALLIFT,
                "7" => RouteType::FUNICULAR,
                "11" => RouteType::TROLLEYBUS,
                "12" => RouteType::MONORAIL,
                _=> panic!("Invalid route_type!")
            } 
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct StopTime {
    pub trip_id: RecordId,
    pub stop_sequence: usize,
    pub stop_id: RecordId,
    pub arrival_time: CSTime,
    pub departure_time: CSTime,
}

impl StopTime {
    pub fn new(source: &str) -> StopTime {
        let parts: Vec<&str> = source.split(",").collect();
        StopTime { 
            trip_id: RecordId::from(("trip", parts[0])), 
            stop_sequence: usize::from_str_radix(parts[1], 10).unwrap(), 
            stop_id: RecordId::from(("stop", parts[2])), 
            arrival_time: CSTime::parse_from_str(parts[4]), 
            departure_time: CSTime::parse_from_str(parts[5]) 
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum LocationType {
    STOP,
    STATION,
    ENTRANCE,
    GENERIC,
    BOARDINGAREA
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Stop {
    pub stop_id: RecordId,
    pub stop_name: String,
    location_type: Option<LocationType>,
    parent_station: String,
    pub platform_code: String
}

impl Stop {
    pub fn new(source: &str) -> Stop {
        let mut parts = Vec::new();
        let mut start = 0;
        let mut in_quotes = false;

        for (i, c) in source.char_indices() {
            match c {
                '"' => in_quotes = !in_quotes,
                ',' if !in_quotes => {
                    parts.push(source[start..i].trim());
                    start = i + 1;
                }
                _ => {}
            }
        }

        parts.push(source[start..].trim());

        Stop { 
            stop_id: RecordId::from(("stop", parts[0])), 
            stop_name: parts[2].to_string(), 
            location_type: match parts[5] {
                "0" => Some(LocationType::STOP),
                "1" => Some(LocationType::STATION),
                "2" => Some(LocationType::ENTRANCE),
                "3" => Some(LocationType::GENERIC),
                "4" => Some(LocationType::BOARDINGAREA),
                _=> panic!("Invalid location_type!")
            }, 
            parent_station: parts[6].to_string(),
            platform_code: parts[9].to_string() 
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub enum TransferType {
    RECOMMENDED,
    TIMED,
    MINIMUMTIME,
    NONE,
    INSEAT,
    REBOARD
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Transfer {
    pub from_stop: RecordId,
    pub to_stops: Vec<RecordId>
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Trip {
    pub route_id: RecordId,
    service_id: String,
    pub trip_id: RecordId,
    trip_headsign: String,
    trip_short_name: String,
    trip_long_name: String
}

impl Trip {
    pub fn new(source: &str) -> Trip {
        let parts: Vec<&str> = source.split(",").collect();
        Trip { 
            route_id: RecordId::from(("route", parts[0])), 
            service_id: parts[1].to_string(), 
            trip_id: RecordId::from(("trip", parts[2])), 
            trip_headsign: parts[4].to_string(), 
            trip_short_name: parts[5].to_string(), 
            trip_long_name: parts[6].to_string() 
        }
    }
}