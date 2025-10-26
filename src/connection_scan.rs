use std::collections::HashMap;

use surrealdb::{engine::remote::ws::Client, Error, RecordId, Surreal};

use crate::models::{gtfs::Stop, CSTime, Connection};

pub fn find_journey(dep_stop: &Stop, arr_stop: &Stop, dep_time: CSTime, transfers: &HashMap<RecordId, Vec<RecordId>>, connections: &Vec<Connection>) -> Vec<(Connection, Connection, (RecordId, RecordId))> {
    let mut stop_arrival_times: HashMap<RecordId, CSTime> = HashMap::new();
    let mut trip_enter_connections: HashMap<RecordId, Connection> = HashMap::new();
    let mut stop_journeys: HashMap<RecordId, (Connection, Connection, (RecordId, RecordId))> = HashMap::new();

    for stop in &transfers[&dep_stop.stop_id] {
        stop_arrival_times.insert(stop.clone(), dep_time);
    }
    
    let first_connection = connections.binary_search_by(|x| x.dep_time.cmp(&dep_time)).unwrap();

    for connection in &connections[first_connection..] {

        if stop_arrival_times.contains_key(&arr_stop.stop_id) && stop_arrival_times[&arr_stop.stop_id] <= connection.dep_time {
            break;
        }

        if trip_enter_connections.contains_key(&connection.trip) || (stop_arrival_times.contains_key(&connection.dep_stop) && stop_arrival_times[&connection.dep_stop] <= connection.dep_time) {
            if !trip_enter_connections.contains_key(&connection.trip) {
                trip_enter_connections.insert(connection.trip.clone(), connection.clone());
            }

            for transfer_stop in &transfers[&connection.arr_stop] {
                if !stop_arrival_times.contains_key(transfer_stop) || (stop_arrival_times.contains_key(transfer_stop) && connection.arr_time < stop_arrival_times[&transfer_stop]) {
                    stop_arrival_times.insert(transfer_stop.clone(), connection.arr_time);
                    stop_journeys.insert(transfer_stop.clone(), (trip_enter_connections[&connection.trip].clone(), connection.clone(), (connection.arr_stop.clone(), transfer_stop.clone())));
                }
            }
        }
    }

    let mut journey = vec![];
    let mut t = arr_stop.stop_id.clone();
    while stop_journeys.contains_key(&t) {
        let journey_leg = stop_journeys[&t].clone();
        journey.push(journey_leg.clone());
        t = journey_leg.0.dep_stop;
    }
    journey.reverse();
    journey
}

pub async fn print_journey(journey: Vec<(Connection, Connection, (RecordId, RecordId))>, db: &Surreal<Client>) -> Result<(), Error> {
    for journey_leg in journey {
        let enter_connection = journey_leg.0;
        let exit_connection = journey_leg.1;
        let footpath = journey_leg.2;
        let dep_stop: Stop = db.select(enter_connection.dep_stop).await?.unwrap();
        let arr_stop: Stop = db.select(exit_connection.arr_stop).await?.unwrap();
        let footpath_begin: Stop = db.select(footpath.0).await?.unwrap();
        let footpath_end: Stop = db.select(footpath.1).await?.unwrap();
        println!("{} spoor {} ({}), {} spoor {} ({})", dep_stop.stop_name, dep_stop.platform_code, enter_connection.dep_time, arr_stop.stop_name, arr_stop.platform_code, exit_connection.arr_time);
        println!("Walk from {} spoor {} to {} spoor {}", footpath_begin.stop_name, footpath_begin.platform_code, footpath_end.stop_name, footpath_end.platform_code);
    }

    Ok(())
}