use std::{cmp::min, collections::{HashMap, HashSet}};

use surrealdb::{engine::remote::ws::Client, Error, RecordId, Surreal};

use crate::models::{gtfs::Stop, CSTime, Connection};

#[derive(Debug, Clone, PartialEq)]
pub enum JourneyLeg {
    CONNECTION(RecordId),
    FOOTPATH(RecordId)
}

impl JourneyLeg {
    pub async fn print_journey(journey: Vec<JourneyLeg>, db: &Surreal<Client>) -> Result<(), Error>{
        for leg in journey {
            match leg {
                JourneyLeg::CONNECTION(record_id) => {
                    let stop: Stop = db.select(record_id).await?.unwrap();
                    println!("Ride train from {} platform {}", stop.stop_name, stop.platform_code);
                },
                JourneyLeg::FOOTPATH(record_id) => {
                    let stop: Stop = db.select(record_id).await?.unwrap();
                    println!("Walk from {} platform {}", stop.stop_name, stop.platform_code);
                },
            }
        }
        Ok(())
    }
}

pub fn earliest_arrival(dep_stop: &Stop, arr_stop: &Stop, dep_time: CSTime, stops: &Vec<Stop>, transfers: &HashMap<RecordId, Vec<RecordId>>, connections: &Vec<Connection>) -> Option<(Vec<JourneyLeg>, CSTime)> {
    let mut stop_arrival_times: HashMap<RecordId, CSTime> = HashMap::new();
    let mut reachable_trips: HashSet<RecordId> = HashSet::new();
    let mut arrival_stops: HashSet<RecordId> = HashSet::new();
    let mut journey_legs: HashMap<RecordId, JourneyLeg> = HashMap::new();

    for stop in stops {
        if stop.stop_name == dep_stop.stop_name && stop.stop_id != dep_stop.stop_id {
            stop_arrival_times.insert(stop.stop_id.clone(), dep_time);
            journey_legs.insert(stop.stop_id.clone(), JourneyLeg::FOOTPATH(dep_stop.stop_id.clone()));
        } else if stop.stop_name == arr_stop.stop_name {
            arrival_stops.insert(stop.stop_id.clone());
        }
    }

    let first_connection = connections.binary_search_by(|x| x.dep_time.cmp(&dep_time)).unwrap();

    for connection in &connections[first_connection..] {

        if arrival_stops.iter().any(|stop| stop_arrival_times.contains_key(stop)) {
            let arrival_times: Vec<CSTime> = arrival_stops.iter().filter_map(|stop| stop_arrival_times.get(stop)).cloned().collect();
            if arrival_times.iter().min() <= Some(&connection.dep_time) {
                let arrival_times_vec: Vec<(RecordId, CSTime)> = arrival_stops.iter().filter(|stop| stop_arrival_times.contains_key(&stop)).map(|stop| (stop.clone(), stop_arrival_times[stop])).collect();
                let final_stop = arrival_times_vec.iter().min_by(|a, b| a.1.cmp(&b.1)).unwrap().0.clone();
                let mut journey = vec![];
                let mut stop = final_stop.clone();
                println!("extracting journey");
                while journey_legs.get(&stop) != None {
                    journey.push(journey_legs[&stop].clone());
                    stop = match &journey_legs[&stop] {
                        JourneyLeg::CONNECTION(record_id) => record_id.clone(),
                        JourneyLeg::FOOTPATH(record_id) => record_id.clone(),
                    };
                }
                journey.reverse();
                return Some((journey, stop_arrival_times[&final_stop]));
            }
        }

        if reachable_trips.contains(&connection.trip) || (stop_arrival_times.contains_key(&connection.dep_stop) && stop_arrival_times[&connection.dep_stop] <= connection.dep_time) {
            reachable_trips.insert(connection.trip.clone());

            if !stop_arrival_times.contains_key(&connection.arr_stop) || (stop_arrival_times.contains_key(&connection.arr_stop) && connection.arr_time < stop_arrival_times[&connection.arr_stop]) {
                stop_arrival_times.insert(connection.arr_stop.clone(), connection.arr_time);
                journey_legs.insert(connection.arr_stop.clone(), JourneyLeg::CONNECTION(connection.dep_stop.clone()));

                for transfer_stop in &transfers[&connection.arr_stop] {
                    if stop_arrival_times.contains_key(&transfer_stop) {
                        if stop_arrival_times[&transfer_stop] > connection.arr_time {
                            journey_legs.insert(transfer_stop.clone(), JourneyLeg::FOOTPATH(connection.arr_stop.clone()));
                        }
                        stop_arrival_times.insert(transfer_stop.clone(), min(stop_arrival_times[&transfer_stop], connection.arr_time));
                    }
                    else {
                        stop_arrival_times.insert(transfer_stop.clone(), connection.arr_time);
                        journey_legs.insert(transfer_stop.clone(), JourneyLeg::FOOTPATH(connection.arr_stop.clone()));
                    }
                }
            }
        }
    }
    None
}