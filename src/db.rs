use std::{collections::{HashMap, HashSet}, fs::File, io::{BufRead, BufReader}};

use futures::{StreamExt, stream};
use surrealdb::{engine::remote::ws::Client, Error, RecordId, Surreal};

use crate::models::{gtfs::{CalendarDate, Route, RouteType, Stop, StopTime, Transfer, TransferType, Trip}, Connection};

fn parse_transfer_line(line: String) -> Option<(RecordId, RecordId)> {
    let parts: Vec<&str> = line.split(',').collect();
    if parts.len() <= 6 { return None; }

    let transfer_type = match parts[6] {
        "0" => TransferType::RECOMMENDED,
        "1" => TransferType::TIMED,
        "2" => TransferType::MINIMUMTIME,
        "3" => TransferType::NONE,
        "4" => TransferType::INSEAT,
        "5" => TransferType::REBOARD,
        _ => return None,
    };

    if transfer_type == TransferType::NONE { return None; }
    Some((
        RecordId::from(("stop", parts[0])),
        RecordId::from(("stop", parts[1])),
    ))
}

pub async fn read_gtfs(path: &str, db: &Surreal<Client>) -> Result<(), Error> {

    let calandar_dates_file = File::open(format!("{}/calendar_dates.txt", path)).unwrap();
    let reader = BufReader::new(calandar_dates_file);

    println!("Reading CalendarDates");
    let calendar_dates: Vec<CalendarDate> = reader.lines().skip(1).filter_map(|l| l.ok().map(CalendarDate::new)).collect();
    let _: Vec<CalendarDate> = db.insert("calendar_date").content(calendar_dates).await?;

    let routes_file = File::open(format!("{}/routes.txt", path)).unwrap();
    let reader = BufReader::new(routes_file);

    println!("Reading Routes");
    let routes: Vec<Route> = reader.lines().skip(1).filter_map(|l| l.ok().map(Route::new)).collect();
    let known_rail_routes: HashSet<RecordId> = routes.iter().filter(|r| r.route_type == RouteType::RAIL).map(|r| r.id.clone()).collect();
    let _: Vec<Route> = db.insert("route").content(routes).await?;

    let stops_file = File::open(format!("{}/stops.txt", path)).unwrap();
    let reader = BufReader::new(stops_file);

    println!("Reading Stops");
    let stops_data: Vec<Stop> = reader.lines().skip(1).filter_map(|l| l.ok().map(Stop::new)).collect();
    let stops: Vec<RecordId> = stops_data.iter().map(|s| s.id.clone()).collect();
    let _: Vec<Stop> = db.insert("stop").content(stops_data).await?;

    let trips_file = File::open(format!("{}/trips.txt", path)).unwrap();
    let reader = BufReader::new(trips_file);

    println!("Reading Trips");
    let trips: Vec<Trip> = reader.lines().skip(1).filter_map(|l| l.ok().map(Trip::new)).filter(|t| known_rail_routes.contains(&t.route_id)).collect();
    let known_rail_trips: HashSet<RecordId> = trips.iter().map(|t| t.id.clone()).collect();
    let trips: Vec<Trip> = db.insert("trip").content(trips).await?;

    println!("Reading Transfers");
    let transfers_file = File::open(format!("{}/transfers.txt", path)).unwrap();
    let reader = BufReader::new(transfers_file);

    let transfers: HashSet<(RecordId, RecordId)> = reader.lines().skip(1).filter_map(|l| l.ok().map(parse_transfer_line).flatten()).collect();
    let transfer_map: HashMap<RecordId, Vec<RecordId>> = {
        let mut map = HashMap::new();
        for (from, to) in transfers {
            map.entry(from).or_insert_with(Vec::new).push(to);
        }
        map
    };
    let transfer_records: Vec<Transfer> = stops.into_iter().filter_map(|stop_id| {
        transfer_map.get(&stop_id).map(|to_stops| Transfer {
            from_stop: stop_id.clone(),
            to_stops: to_stops.clone(),
        })
    }).collect();
    let _: Vec<Transfer> = db.insert("transfer").content(transfer_records).await?;


    let stop_times_file = File::open(format!("{}/stop_times.txt", path)).unwrap();
    let reader = BufReader::new(stop_times_file);

    println!("Reading StopTimes");
    let stop_times: Vec<StopTime> = reader.lines()
        .skip(1)
        .filter_map(|l| l.ok())
        .filter(|line| {
            let id = RecordId::from(("trip", &line[..9]));
            known_rail_trips.contains(&id)
        })
        .map(|line| StopTime::new(&line))
        .collect();
    stream::iter(stop_times.clone()).for_each_concurrent(16, |stop_time| async {
        let _: Result<Option<StopTime>, _> = db.create("stop_time").content(stop_time).await;
    }).await;


    println!("Building Connections");
    Connection::build_connections(db, trips, stop_times).await?;

    Ok(())
}