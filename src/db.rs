use std::collections::{HashMap, HashSet};

use csv::{ReaderBuilder, StringRecord};
use futures::{StreamExt, stream};
use surrealdb::{engine::remote::ws::Client, Error, RecordId, Surreal};

use crate::models::{gtfs::{CalendarDate, Route, RouteType, Stop, StopTime, Transfer, TransferType, Trip}, Connection};

const CHUNK_SIZE: usize = 1000;

fn parse_transfer_line(line: StringRecord) -> Option<(RecordId, RecordId)> {
    let transfer_type = match &line[6] {
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
        RecordId::from(("stop", &line[0])),
        RecordId::from(("stop", &line[1])),
    ))
}

pub async fn read_gtfs(path: &str, db: &Surreal<Client>) -> Result<(), Error> {

    println!("Reading CalendarDates");
    let mut reader = ReaderBuilder::new().has_headers(true).from_path(format!("{}/calendar_dates.txt", path)).unwrap();
    let calendar_dates: Vec<CalendarDate> = reader.records().filter_map(|l| l.ok().map(CalendarDate::new)).collect();
    let _: Vec<CalendarDate> = db.insert("calendar_date").content(calendar_dates).await?;

    println!("Reading Routes");
    let mut reader = ReaderBuilder::new().has_headers(true).from_path(format!("{}/routes.txt", path)).unwrap();
    let routes: Vec<Route> = reader.records().filter_map(|l| l.ok().map(Route::new)).collect();
    let known_rail_routes: HashSet<RecordId> = routes.iter().filter(|r| r.route_type == RouteType::RAIL).map(|r| r.id.clone()).collect();
    let _: Vec<Route> = db.insert("route").content(routes).await?;

    println!("Reading Stops");
    let mut reader = ReaderBuilder::new().has_headers(true).from_path(format!("{}/stops.txt", path)).unwrap();
    let stops_data: Vec<Stop> = reader.records().filter_map(|l| l.ok().map(Stop::new)).collect();
    let stops: Vec<RecordId> = stops_data.iter().map(|s| s.id.clone()).collect();
    let _: Vec<Stop> = db.insert("stop").content(stops_data).await?;

    println!("Reading Trips");
    let mut reader = ReaderBuilder::new().has_headers(true).from_path(format!("{}/trips.txt", path)).unwrap();
    let trips: Vec<Trip> = reader.records().filter_map(|l| l.ok().map(Trip::new)).filter(|t| known_rail_routes.contains(&t.route_id)).collect();
    let known_rail_trips: HashSet<RecordId> = trips.iter().map(|t| t.id.clone()).collect();
    let trips: Vec<Trip> = db.insert("trip").content(trips).await?;

    println!("Reading Transfers");
    let mut reader = ReaderBuilder::new().has_headers(true).from_path(format!("{}/transfers.txt", path)).unwrap();
    let transfers: HashSet<(RecordId, RecordId)> = reader.records().filter_map(|l| l.ok().map(parse_transfer_line).flatten()).collect();
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

    println!("Reading StopTimes");
    let mut reader = ReaderBuilder::new().has_headers(true).from_path(format!("{}/stop_times.txt", path)).unwrap();
    let stop_times: Vec<StopTime> = reader.records()
        .filter_map(|l| l.ok())
        .filter(|line| {
            let id = RecordId::from(("trip", &line[0]));
            known_rail_trips.contains(&id)
        })
        .map(|line| StopTime::new(line))
        .collect();
    let stop_time_chunks: Vec<_> = stop_times.chunks(CHUNK_SIZE).map(|c| c.to_vec()).collect();

    stream::iter(stop_time_chunks).for_each_concurrent(16, |chunk| async move {
        let _: Result<Option<Vec<StopTime>>, _> = db.create("stop_time").content(chunk).await;
    }).await;


    println!("Building Connections");
    Connection::build_connections(db, trips, stop_times).await?;

    Ok(())
}