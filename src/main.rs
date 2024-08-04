pub mod encapsulators;

use chrono::NaiveDate;
use clap::Parser;
use csv;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::sync::mpsc;
use std::{thread, u16};

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    csv_path: String,
}

fn parse_date_from_text(text_date: &str) -> Option<NaiveDate> {
    let mut date_assembler = encapsulators::PossibleDate::new();

    let split_date_iterator = text_date.split(|c: char| !c.is_digit(10)).into_iter();

    let mut loop_counter: u8 = 0;
    // go over the split numbers and assemble date
    for number_sequence in split_date_iterator {
        loop_counter += 1;
        // we should have the whole date after 3 iterations, so we can break out of this loop
        if loop_counter == 4 {
            break;
        }
        match number_sequence.len() {
            // year-like structure passed
            4 => {
                // if year is empty
                if date_assembler.get_year() == None {
                    // put current sequence as year
                    let parsed = number_sequence.parse::<i32>();
                    match parsed {
                        Ok(year) => date_assembler.set_year(year),
                        Err(_) => return None,
                    }
                }
            }
            _ => {
                // if year is empty, the format is d(d)-(m)m-yyyy
                if date_assembler.get_year() == None {
                    // if year and day are empty, day was passed during first iteration
                    if date_assembler.get_day() == None {
                        // put current sequence as day
                        let parsed = number_sequence.parse::<u32>();
                        match parsed {
                            Ok(day) => date_assembler.set_day(day),
                            Err(_) => return None,
                        }
                        // if year is empty, but day filled, month was passed during second iteration
                    } else {
                        // put current sequence as month
                        let parsed = number_sequence.parse::<u32>();
                        match parsed {
                            Ok(month) => date_assembler.set_month(month),
                            Err(_) => return None,
                        }
                    }
                } else {
                    // if year is filled, the format is yyyy-(m)m-(d)d
                    // if year is filled, but month is empty, month was passed during second
                    // iteration
                    if date_assembler.get_month() == None {
                        // put current sequence as month
                        let parsed = number_sequence.parse::<u32>();
                        match parsed {
                            Ok(month) => date_assembler.set_month(month),
                            Err(_) => return None,
                        }
                    // if year and month are filled, day was passed during third iteration
                    } else {
                        // put current sequance as day
                        let parsed = number_sequence.parse::<u32>();
                        match parsed {
                            Ok(day) => date_assembler.set_day(day),
                            Err(_) => return None,
                        }
                    }
                }
            }
        };
    }

    let year_exists = date_assembler.get_year() != None;
    let month_exists = date_assembler.get_month() != None;
    let day_exists = date_assembler.get_day() != None;

    if year_exists && month_exists && day_exists {
        let constructed_date = NaiveDate::from_ymd_opt(
            date_assembler.get_year().unwrap(),
            date_assembler.get_month().unwrap(),
            date_assembler.get_day().unwrap(),
        );
        return constructed_date;
    } else {
        return None;
    }
}

/// reads a csv file, and returns a file reader and vector with all headers
fn load_file(path: &str) -> Result<(csv::Reader<File>, Vec<String>), csv::Error> {
    let mut reader = csv::Reader::from_path(path)?;
    let header = reader.headers()?; // get headers

    // makes a vec of owned strings
    let string_headers: Vec<String> = header.iter().map(|x| x.to_owned()).collect();

    return Ok((reader, string_headers.to_owned()));
}

/// extracts median value from hashmap with string representation of float values and the number of
/// their occurences
fn get_stats_from_hashmap(mut hashmap: HashMap<String, u16>) -> f64 {
    // parse all keys into float values
    let mut keys: Vec<f64> = hashmap
        .keys()
        .map(|key| key.parse::<f64>().unwrap())
        .collect();

    // sort the keys
    let count_distinct = keys.len();
    keys.sort_by(|a, b| a.partial_cmp(b).unwrap());

    // set up the algorithm for finding the mean
    let mut left: usize = 0;
    let mut right: usize = count_distinct - 1;
    let mut left_value = keys[left];
    let mut right_value = keys[right];

    // left < right means this is before the half way, where we compare the same values or compare
    // the same stuff, or the same stuff we already compared
    while left < right {
        // whether there are any occurences of this value left in the hashmap
        if hashmap[&left_value.to_string()] > 0 {
            // if yes, substract 1 from the occurences count for this value
            hashmap.insert(left_value.to_string(), hashmap[&left_value.to_string()] - 1);
        } else {
            // if no occurences of this value are left, move to a higher value as we aproach the
            // center from left here, (and swap the left value to match the new left index
            left += 1;
            left_value = keys[left];
        }

        // whether there are any occurences of this value left in the hashmap
        if hashmap[&right_value.to_string()] > 0 {
            // if yes, substract 1 from the occurences count for this value
            hashmap.insert(
                right_value.to_string(),
                hashmap[&right_value.to_string()] - 1,
            );
        } else {
            // if no occurences of this value are left, move to a lower value as we approach the
            // center from right here, (and swap the value to match the new right index)
            right -= 1;
            right_value = keys[right];
        }
    }

    let median: f64;

    if left == right {
        median = left_value;
    } else {
        // if left and right are different values (even number count, and lack of one center value),
        // take the mean of those
        median = (left_value + right_value) / 2.0;
    }

    return median;
}

/// creates a thread for each column in the file (based on headers vector), then each thread iterates over the column values
/// and creates aggregate statistics. Returns two hashmaps: one for handles for all threads, and
/// one with senders that pass the column values to the threads. Both hashmaps are searchable using
/// headers (column names)
fn get_hashmaps(
    headers: &Vec<String>,
) -> (
    HashMap<&String, thread::JoinHandle<encapsulators::ColumnSummary>>,
    HashMap<&String, mpsc::Sender<encapsulators::ColumnType>>,
) {
    // create empty hash maps for handles and senders
    let mut handles_map: HashMap<&String, thread::JoinHandle<encapsulators::ColumnSummary>> =
        HashMap::new();
    let mut sender_map: HashMap<&String, mpsc::Sender<encapsulators::ColumnType>> = HashMap::new();

    // handle each column
    for header in headers {
        // get sender and receiver that handles ColumnType object
        let (tx, rx) = mpsc::channel::<encapsulators::ColumnType>();

        // create a thread for a given column
        let handle = thread::spawn(move || {
            // assumes the column has text values, used later to return the right thing
            let mut text_column: bool = true;
            let mut date_column: bool = false;

            // create empty hashmap and category count for text values
            let mut categories: HashSet<String> = HashSet::new();
            let mut category_count: u16 = 0;

            // set up agg variables for numerical column
            let mut sum: f64 = 0.0;

            // variables for one-pass standard deviation calculation
            let mut m: f64 = 0.0;
            let mut s: f64 = 0.0;
            let mut row_counter: u64 = 0;

            // hashmap that will store string representation of float values with the counter to
            // calculate the median later
            let mut mode_map: HashMap<String, u16> = HashMap::new();

            // struct storing the earliest and latest date from file
            let mut date_aggregate = encapsulators::DateColumn::new();

            // wait for messages from row splitting part and handle values when they arrive
            for message in rx {
                row_counter += 1;

                // match message based on the value type
                match message {
                    encapsulators::ColumnType::Float(number_value) => {
                        // handle int/float values: add to sum, put the value in mode hashmap,
                        // handle calculating one pass standard deviation
                        text_column = false;

                        sum = sum + number_value;
                        mode_map.entry(number_value.to_string()).or_insert(0);
                        mode_map.insert(
                            number_value.to_string(),
                            mode_map[&number_value.to_string()] + 1,
                        );

                        // std calculation
                        let old_m = m;
                        m = m + (number_value - m) / row_counter as f64;
                        s = s + (number_value - m) * (number_value - old_m)
                    }
                    encapsulators::ColumnType::Date(date_value) => {
                        // TODO: something is wrong here with how earliest and latest is passed to
                        // date_aggregate

                        // mark this as a date column
                        date_column = true;
                        text_column = false;

                        // swap earliest if new earlies date found
                        let current_earliest = date_aggregate.get_earliest();
                        match current_earliest {
                            Some(date) => {
                                if date_value < date {
                                    date_aggregate.set_earliest(date_value);
                                }
                            }
                            None => date_aggregate.set_earliest(date_value),
                        }

                        // swap latest if new latest date found
                        let current_latest = date_aggregate.get_latest();
                        match current_latest {
                            Some(date) => {
                                if date_value > date {
                                    date_aggregate.set_latest(date_value)
                                }
                            }
                            None => date_aggregate.set_latest(date_value),
                        }
                    }
                    encapsulators::ColumnType::Text(text_value) => {
                        // handle text values: add category to counter and hashmap if they are not
                        // already there
                        if !categories.contains(&text_value) {
                            categories.insert(text_value);
                            category_count += 1;
                        }
                    }
                    // FileEnd is passed only on EOF to exit the loop
                    encapsulators::ColumnType::FileEnd => break,
                }
            }
            // decrease row_counter by 1 because FileEnd value does not represent an actual file
            // row
            row_counter -= 1;

            // return ColumnSummary value based on handles column type
            if text_column == true {
                let mut text_column_summary = encapsulators::TextColumn::new();
                text_column_summary.set_categories(categories);
                text_column_summary.set_category_count(category_count);

                return encapsulators::ColumnSummary::Text(text_column_summary);
            } else if date_column == true {
                let mut date_column_summary = encapsulators::DateColumn::new();
                date_column_summary.set_earliest(date_aggregate.get_earliest().unwrap());
                date_column_summary.set_latest(date_aggregate.get_latest().unwrap());
                return encapsulators::ColumnSummary::Date(date_column_summary);
            } else {
                // calculate summary statistics
                let mean = sum / row_counter as f64;
                let std = s / (row_counter - 1) as f64;
                let median = get_stats_from_hashmap(mode_map);

                let mut number_column_summary = encapsulators::NumberColumn::new();
                number_column_summary.set_sum(sum);
                number_column_summary.set_mean(mean);
                number_column_summary.set_median(median);
                number_column_summary.set_std(std);
                return encapsulators::ColumnSummary::Number(number_column_summary);
            }
        });
        // put thread's handle and sender in hashmaps
        handles_map.insert(header, handle);
        sender_map.insert(&header, tx);
    }

    return (handles_map, sender_map);
}

/// displays the summary statistics for both text and number columns given vectors of tuples with
/// column name and aggregated values. Numbers have precision of 4 decimal spaces
fn display_stats(
    text_summary: Vec<(String, encapsulators::TextColumn)>,
    number_summary: Vec<(String, encapsulators::NumberColumn)>,
    date_summary: Vec<(String, encapsulators::DateColumn)>,
) {
    println!("Text columns\n");
    println!("column               class count          classes");
    for (column_name, column_stats) in text_summary {
        let categories = &column_stats.get_categories();
        let vec_categories: Vec<&String> = categories.iter().collect();
        let count = column_stats.get_category_count();
        if count > 10 {
            println!("{:<20} {:<20} (a lot)", column_name, count)
        } else {
            println!("{:<20} {:<20} ({:<20?})", column_name, count, vec_categories)
        }
    }

    println!("\nDate columns\n");
    println!("column              earliest            latest");
    for (column_name, column_stats) in date_summary {
        println!(
            "{:<20}{:<20}{:<20}",
            column_name,
            format!("{:<20}", column_stats.get_earliest().unwrap()),
            format!("{:<20}", column_stats.get_latest().unwrap())
        )
    }

    println!("\nNumber columns\n");
    println!("column              sum                 mean                median              std");
    for (column_name, column_stats) in number_summary {
        println!(
            "{:<20}{:<20.4}{:<20.4}{:<20.4}{:<20.4}",
            column_name,
            column_stats.get_sum(),
            column_stats.get_mean(),
            column_stats.get_median(),
            column_stats.get_std()
        )
    }
}

fn main() {
    // parses any arguments
    let args = Args::parse();
    let (file_reader, headers) = load_file(&args.csv_path).expect("Failed to load the file");

    let (mut handles_map, senders_map) = get_hashmaps(&headers);

    // read the csv line by line, and send the values to respective threads
    for line in file_reader.into_records() {
        let clean_line = line.expect("Failed to get line");

        // get index of the column name and the column name
        for (index, header) in headers.iter().enumerate() {
            // get value of a given column in a given line
            let value = clean_line
                .get(index)
                .expect("Failed to get value from index");

            // try to convert the value to float, if succeeds, use Float type, if it throws an
            // error, use Text type
            let converted_value = match value.parse::<f64>() {
                Ok(v) => encapsulators::ColumnType::Float(v),
                Err(_) => {
                    let opt_date = parse_date_from_text(value);
                    match opt_date {
                        Some(d) => encapsulators::ColumnType::Date(d),
                        None => encapsulators::ColumnType::Text(value.to_owned()),
                    }
                }
            };

            // send the value to the thread that manages this column
            match senders_map[header].send(converted_value) {
                Ok(_) => {}
                Err(e) => {
                    println!("Skipped a row because of sending to thread problem: {}", e);
                }
            }
        }
    }

    // send a FileEnd message to every thread, so that they stop working
    for header in headers.iter() {
        match senders_map[header].send(encapsulators::ColumnType::FileEnd) {
            Ok(_) => {}
            Err(e) => {
                println!(
                    "Did not end iter end messgage because of sending to thread problem: {}",
                    e
                );
            }
        }
    }

    // prepare empty vectors for column summaries of different types
    let mut text_summary: Vec<(String, encapsulators::TextColumn)> = vec![];
    let mut number_summary: Vec<(String, encapsulators::NumberColumn)> = vec![];
    let mut date_summary: Vec<(String, encapsulators::DateColumn)> = vec![];

    // joins all threads back into main, pushes the returned value to appropriate column type
    // vector
    for header in headers.iter() {
        let column_summary = handles_map
            .remove(header)
            .expect("Did not get a handle from header")
            .join();

        match column_summary {
            Ok(join_handle) => match join_handle {
                encapsulators::ColumnSummary::Text(text_column) => text_summary.push((
                    header.to_owned(),
                    text_column.build_summary()
                )),
                encapsulators::ColumnSummary::Number(number_column) => number_summary.push((
                    header.to_owned(),
                    number_column.build_summary(),
                )),
                encapsulators::ColumnSummary::Date(date_column) => date_summary.push((
                    header.to_owned(),
                    date_column.build_summary(),
                )),
            },
            Err(_) => println!("Something went wrong during joining {} handle", &header),
        }
    }

    // displays all the results
    display_stats(text_summary, number_summary, date_summary);
}
