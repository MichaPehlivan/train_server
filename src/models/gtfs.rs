use chrono::NaiveDate;
use csv::StringRecord;
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
    pub service_id: String,
    date: NaiveDate,
    excpetion_type: ExceptionType
}

impl CalendarDate {
    pub fn new(source: StringRecord) -> CalendarDate {
        CalendarDate { 
            service_id: source[0].to_string(), 
            date: NaiveDate::parse_from_str(&source[1], "%Y%m%d").unwrap(), 
            excpetion_type: match &source[2] {
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
    pub id: RecordId,
    agency_id: String,
    route_short_name: String,
    route_long_name: String,
    pub route_type: RouteType
}

impl Route {
    pub fn new(source: StringRecord) -> Route {
        Route { 
            id: RecordId::from(("route", &source[0])), 
            agency_id: source[1].to_string(), 
            route_short_name: source[2].to_string(), 
            route_long_name: source[3].to_string(), 
            route_type: match &source[5] {
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
    pub fn new(source: StringRecord) -> StopTime {
        StopTime { 
            trip_id: RecordId::from(("trip", &source[0])), 
            stop_sequence: usize::from_str_radix(&source[1], 10).unwrap(), 
            stop_id: RecordId::from(("stop", &source[2])), 
            arrival_time: CSTime::parse_from_str(&source[4]), 
            departure_time: CSTime::parse_from_str(&source[5]) 
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub enum LocationType {
    STOP,
    STATION,
    ENTRANCE,
    GENERIC,
    BOARDINGAREA
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct Stop {
    pub id: RecordId,
    pub stop_name: String,
    pub location_type: LocationType,
    parent_station: String,
    pub platform_code: String
}

impl Stop {
    pub fn new(source: StringRecord) -> Stop {
        Stop { 
            id: RecordId::from(("stop", &source[0])), 
            stop_name: source[2].to_string(), 
            location_type: match &source[5] {
                "0" => LocationType::STOP,
                "1" => LocationType::STATION,
                "2" => LocationType::ENTRANCE,
                "3" => LocationType::GENERIC,
                "4" => LocationType::BOARDINGAREA,
                _=> panic!("Invalid location_type!")
            }, 
            parent_station: source[6].to_string(),
            platform_code: source[9].to_string() 
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
    pub id: RecordId,
    pub route_id: RecordId,
    pub service_id: String,
    trip_headsign: String,
    trip_short_name: String,
    trip_long_name: String
}

impl Trip {
    pub fn new(source: StringRecord) -> Trip {
        Trip { 
            id: RecordId::from(("trip", &source[2])), 
            route_id: RecordId::from(("route", &source[0])), 
            service_id: source[1].to_string(), 
            trip_headsign: source[4].to_string(), 
            trip_short_name: source[5].to_string(), 
            trip_long_name: source[6].to_string() 
        }
    }
}