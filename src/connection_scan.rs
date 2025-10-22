use std::{cmp::min, collections::{HashMap, HashSet}};

use surrealdb::RecordId;

use crate::models::{gtfs::{Stop, Transfer}, CSTime, Connection};

pub fn earliest_arrival(dep_stop: Stop, arr_stop: Stop, dep_time: CSTime, stops: Vec<Stop>, transfers: &Vec<Transfer>, connections: Vec<Connection>) -> Option<CSTime> {
    let mut stop_arrival_times: HashMap<RecordId, CSTime> = HashMap::new();
    let mut reachable_trips: HashSet<RecordId> = HashSet::new();
    let mut arrival_stops: HashSet<RecordId> = HashSet::new();

    for stop in &stops {
        if stop.stop_name == dep_stop.stop_name {
            stop_arrival_times.insert(stop.stop_id.clone(), dep_time.clone());
        }
        if stop.stop_name == arr_stop.stop_name {
            arrival_stops.insert(stop.stop_id.clone());
        }
    }

    let first_connection = connections.binary_search_by(|x| x.dep_time.cmp(&dep_time)).unwrap();

    for connection in &connections[first_connection..] {
        
        /*if stop_arrival_times.contains_key(&arr_stop.stop_id) && stop_arrival_times[&arr_stop.stop_id] <= connection.dep_time {
            return Some(stop_arrival_times[&arr_stop.stop_id].clone());
        }*/

        if arrival_stops.iter().any(|stop| stop_arrival_times.contains_key(stop)) {
            let arrival_times: Vec<CSTime> = arrival_stops.iter().filter_map(|stop| stop_arrival_times.get(stop)).cloned().collect();
            if arrival_times.iter().min() <= Some(&connection.dep_time) {
                return arrival_times.iter().min().cloned();
            }
        }

        if reachable_trips.contains(&connection.trip) || (stop_arrival_times.contains_key(&connection.dep_stop) && stop_arrival_times[&connection.dep_stop] <= connection.dep_time) {
            reachable_trips.insert(connection.trip.clone());

            if !stop_arrival_times.contains_key(&connection.arr_stop) || (stop_arrival_times.contains_key(&connection.arr_stop) && connection.arr_time < stop_arrival_times[&connection.arr_stop]) {
                stop_arrival_times.insert(connection.arr_stop.clone(), connection.arr_time.clone());

                for transfer in transfers {
                    if transfer.from_stop_id == connection.arr_stop {
                        if stop_arrival_times.contains_key(&transfer.to_stop_id) {
                            stop_arrival_times.insert(transfer.to_stop_id.clone(), min(stop_arrival_times[&transfer.to_stop_id].clone(), connection.arr_time.clone()));
                        }
                        else {
                            stop_arrival_times.insert(transfer.to_stop_id.clone(), connection.arr_time.clone());
                        }
                    }
                }
            }
        }
    }

    None
}