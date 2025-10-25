use std::collections::HashMap;
use std::env;

use surrealdb::engine::remote::ws::Ws;
use surrealdb::opt::auth::Root;
use surrealdb::{RecordId, Surreal};

use crate::connection_scan::{earliest_arrival, find_journey, print_journey};
use crate::models::gtfs::{Stop, Transfer};
use crate::models::{CSTime, Connection};

mod models;
mod db;
mod connection_scan;

#[tokio::main]
async fn main() -> surrealdb::Result<()> {
    let db = Surreal::new::<Ws>("127.0.0.1:8000").await?;

    db.signin(Root {
        username: "root",
        password: "secret",
    })
    .await?;

    db.use_ns("trains").use_db("trains").await?;

    let args: Vec<String> = env::args().collect();
    if args.contains(&"build_db".to_string()) { //Build database if flag is present
        db::read_gtfs("src/gtfs-nl", &db).await?;
        println!("Database filled!");
    }

    let stops: Vec<Stop> = db.select("stop").await?;
    println!("Found Stops");
    let transfer_vec: Vec<Transfer> = db.select("transfer").await?;
    let transfers: HashMap<RecordId, Vec<RecordId>> = transfer_vec.iter().map(|t| (t.from_stop.clone(), t.to_stops.clone())).collect();
    println!("Found Transfers");
    let mut connections: Vec<Connection> = db.query("SELECT * FROM connection WHERE dep_time.hours < 10 ORDER BY dep_time;").await?.take(0).unwrap();
    let mut noon_connections: Vec<Connection> = db.query("SELECT * FROM connection WHERE dep_time.hours >= 10 AND dep_time.hours < 14 ORDER BY dep_time;").await?.take(0).unwrap();
    let mut afternoon_connections: Vec<Connection> = db.query("SELECT * FROM connection WHERE dep_time.hours >= 14 AND dep_time.hours < 18 ORDER BY dep_time;").await?.take(0).unwrap();
    let mut evening_connections: Vec<Connection> = db.query("SELECT * FROM connection WHERE dep_time.hours >= 18 AND dep_time.hours < 22 ORDER BY dep_time;").await?.take(0).unwrap();
    let mut night_connections: Vec<Connection> = db.query("SELECT * FROM connection WHERE dep_time.hours >= 22 ORDER BY dep_time;").await?.take(0).unwrap();
    connections.append(&mut noon_connections);
    connections.append(&mut afternoon_connections);
    connections.append(&mut evening_connections);
    connections.append(&mut night_connections);
    println!("Found Connections");

    let dep_stop: Stop = db.select(("stop", "3065387")).await?.unwrap();
    let arr_stop: Stop = db.select(("stop", "2992891")).await?.unwrap();
    let dep_time = CSTime::parse_from_str("12:00:00");
    println!("Finding earliest arrival time from {} to {} at {}", dep_stop.stop_name, arr_stop.stop_name, dep_time);

    let arrival = earliest_arrival(&dep_stop, &arr_stop, dep_time, &stops, &transfers, &connections);
    match arrival {
        Some(time) => println!("Earliest arrival: {}", time),
        None => println!("No route found :("),
    }

    let dep_stop: Stop = db.select(("stop", "3065387")).await?.unwrap();
    let arr_stop: Stop = db.select(("stop", "2992891")).await?.unwrap();
    let dep_time = CSTime::parse_from_str("12:00:00");
    println!("Finding journey from {} to {} at {}", dep_stop.stop_name, arr_stop.stop_name, dep_time);
    let journey = find_journey(&dep_stop, &arr_stop, dep_time, &stops, &transfers, &connections);
    print_journey(journey, &db).await?;

    Ok(())
}