use std::{collections::HashSet, fs::File, io::{BufRead, BufReader}};

use surrealdb::{engine::remote::ws::Client, Error, RecordId, Surreal};

use crate::models::{gtfs::{CalendarDate, Route, RouteType, Stop, StopTime, Transfer, Trip}, Connection};


pub async fn read_gtfs(path: &str, db: &Surreal<Client>) -> Result<(), Error> {

    let calandar_dates_file = File::open(format!("{}/calendar_dates.txt", path)).unwrap();
    let reader = BufReader::new(calandar_dates_file);

    println!("Reading CalendarDates");
    for line in reader.lines().skip(1) {
        let line = line.unwrap();
        let _: Option<CalendarDate> = db.create("calendar_date").content(CalendarDate::new(&line)).await?;
    }

    let routes_file = File::open(format!("{}/routes.txt", path)).unwrap();
    let reader = BufReader::new(routes_file);
    let mut known_rail_routes: HashSet<RecordId> = HashSet::new();

    println!("Reading Routes");
    for line in reader.lines().skip(1) {
        let line = line.unwrap();
        let route = Route::new(&line);
        if route.route_type == RouteType::RAIL {
            known_rail_routes.insert(route.route_id.clone());
            let _: Option<Route> = db.create(route.route_id.clone()).content(route).await?;
        }
    }

    let stops_file = File::open(format!("{}/stops.txt", path)).unwrap();
    let reader = BufReader::new(stops_file);

    println!("Reading Stops");
    for line in reader.lines().skip(1) {
        let line = line.unwrap();
        let stop = Stop::new(&line);
        let _: Option<Stop> = db.create(stop.stop_id.clone()).content(stop).await?;
    }

    let trips_file = File::open(format!("{}/trips.txt", path)).unwrap();
    let reader = BufReader::new(trips_file);
    let mut known_rail_trips: HashSet<RecordId> = HashSet::new();
    let mut trips: Vec<Trip> = vec![];

    println!("Reading Trips");
    for line in reader.lines().skip(1) {
        let line = line.unwrap();
        let trip = Trip::new(&line);
        if known_rail_routes.contains(&trip.route_id) {
            known_rail_trips.insert(trip.trip_id.clone());
            trips.push(trip.clone());
            let _: Option<Trip> = db.create(trip.trip_id.clone()).content(trip).await?;
        }
    }

    let transfers_file = File::open(format!("{}/transfers.txt", path)).unwrap();
    let reader = BufReader::new(transfers_file);

    println!("Reading Transfers");
    for line in reader.lines().skip(1) {
        let line = line.unwrap();
        let _: Option<Transfer> = db.create("transfer").content(Transfer::new(&line)).await?;
    }

    let stop_times_file = File::open(format!("{}/stop_times.txt", path)).unwrap();
    let reader = BufReader::new(stop_times_file);
    let mut stop_times: Vec<StopTime> = vec![];

    println!("Reading StopTimes");
    for line in reader.lines().skip(1) {
        let line = line.unwrap();
        if known_rail_trips.contains(&RecordId::from(("trip", &line[..9]))) {
            let stop_time = StopTime::new(&line);
            stop_times.push(stop_time.clone());
            let _: Option<StopTime> = db.create("stop_time").content(stop_time).await?;
        }
    }

    println!("Building Connections");
    Connection::build_connections(db, trips, stop_times).await?;

    Ok(())
}