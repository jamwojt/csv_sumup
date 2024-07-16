use std::fs::File;
use std::thread;
use csv;
use std::sync::mpsc;
use std::collections::{HashMap, HashSet};
use clap::Parser;


#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args{
    csv_path: String
}

// stores value from one field
enum ColumnType{
    Text(String),
    Float(f64),
    Date,
    FileEnd
}

// stores summary of a column with text values
struct TextColumn{
    categories: HashSet<String>,
    category_count: u16
}

// stores summary of a column with number values
struct NumberColumn{
    sum: f64,
    mean: f64,
    median: f64,
    std: f64
}

// stores summary of any column
enum ColumnSummary{
    Text(TextColumn),
    Number(NumberColumn)
}

/// reads a csv file, and returns a file reader and vector with all headers
fn load_file(path: &str) -> Result<(csv::Reader<File>, Vec<String>), csv::Error>{
    let mut reader = csv::Reader::from_path(path)?;
    let header =  reader.headers()?; // get headers

    // makes a vec of owned strings
    let string_headers: Vec<String> = header
        .iter()
        .map(|x| x.to_owned())
        .collect();

    return Ok((reader, string_headers.to_owned()));
}

/// extracts median value from hashmap with string representation of float values and the number of
/// their occurences
fn get_stats_from_hashmap(mut hashmap: HashMap<String, u16>) -> f64{
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
    while left < right{
        // whether there are any occurences of this value left in the hashmap
        if hashmap[&left_value.to_string()] > 0{
            
            // if yes, substract 1 from the occurences count for this value
            hashmap.insert(left_value.to_string(), hashmap[&left_value.to_string()] - 1);
        } else{

            // if no occurences of this value are left, move to a higher value as we aproach the
            // center from left here, (and swap the left value to match the new left index
            left += 1;
            left_value = keys[left];
        }

        // whether there are any occurences of this value left in the hashmap
        if hashmap[&right_value.to_string()] > 0{
            
            // if yes, substract 1 from the occurences count for this value
            hashmap.insert(right_value.to_string(), hashmap[&right_value.to_string()] - 1);
        } else{

            // if no occurences of this value are left, move to a lower value as we approach the
            // center from right here, (and swap the value to match the new right index)
            right -= 1;
            right_value = keys[right];
        }
    }

    let median: f64;

    if left == right{
        median = left_value;
    } else{
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
fn get_hashmaps(headers: &Vec<String>) -> (HashMap<&String, thread::JoinHandle<ColumnSummary>>, HashMap<&String, mpsc::Sender<ColumnType>>){
    // create empty hash maps for handles and senders
    let mut handles_map: HashMap<&String, thread::JoinHandle<ColumnSummary>> = HashMap::new();
    let mut sender_map: HashMap<&String, mpsc::Sender<ColumnType>> = HashMap::new();
    
    // handle each column
    for header in headers{
        // get sender and receiver that handles ColumnType object
        let (tx, rx) = mpsc::channel::<ColumnType>();

        // create a thread for a given column 
        let handle = thread::spawn(move ||{
            // assumes the column has text values, used later to return the right thing
            let mut text_column: bool = true;

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

            // wait for messages from row splitting part and handle values when they arrive
            for message in rx{
                row_counter += 1;

                // match message based on the value type
                match message{
                    ColumnType::Float(v) => {
                        // handle int/float values: add to sum, put the value in mode hashmap,
                        // handle calculating one pass standard deviation
                        text_column = false;

                        sum = sum + v;
                        mode_map.entry(v.to_string()).or_insert(0);
                        mode_map.insert(v.to_string(), mode_map[&v.to_string()] + 1);

                        // std calculation
                        let old_m = m;
                        m = m + (v - m) / row_counter as f64;
                        s = s + (v - m) * (v - old_m)

                    },
                    ColumnType::Text(v) => {
                        // handle text values: add category to counter and hashmap if they are not
                        // already there
                        if !categories.contains(&v){
                            categories.insert(v);
                            category_count += 1;
                        }
                    },
                    // FileEnd is passed only on EOF to exit the loop
                    ColumnType::FileEnd => break
                }
            };
            // decrease row_counter by 1 because FileEnd value does not represent an actual file
            // row
            row_counter -= 1; 

            // return ColumnSummary value based on handles column type
            if text_column == true{
                return ColumnSummary::Text(TextColumn{
                    categories: categories,
                    category_count: category_count
                })
            } else{
                // calculate summary statistics
                let mean = sum / row_counter as f64;
                let std = s / (row_counter -1) as f64;
                let median = get_stats_from_hashmap(mode_map);

                return ColumnSummary::Number(NumberColumn{
                    sum: sum,
                    mean: mean,
                    median: median,
                    std: std
                })
            }
        });
        // put thread's handle and sender in hashmaps
        handles_map.insert(header, handle);
        sender_map.insert(&header,tx);
    }


    return (handles_map, sender_map);
}


/// displays the summary statistics for both text and number columns given vectors of tuples with
/// column name and aggregated values. Numbers have precision of 4 decimal spaces
fn display_stats(text_summary: Vec<(String, TextColumn)>, number_summary: Vec<(String, NumberColumn)>){
    println!("Text columns\n");
    println!("column               class count          classes");
    for (column_name, column_stats) in text_summary{
        let categories: Vec<&String> = column_stats.categories.iter().collect();
        let count = column_stats.category_count;
        if count > 10{
            println!("{:<20} {:<20} (a lot)", column_name, count)    
        } else{
            println!("{:<20} {:<20} ({:<20?})", column_name, count, categories)
        }
    }

    println!("\nNumber columns\n");
    println!("column              sum                 mean                median              std");
    for (column_name, column_stats) in number_summary{
        println!(
            "{:<20}{:<20.4}{:<20.4}{:<20.4}{:<20.4}",
            column_name,
            column_stats.sum,
            column_stats.mean,
            column_stats.median,
            column_stats.std
        )

    }
}


fn main() {
    // parses any arguments
    let args = Args::parse();
    let (file_reader, headers) = load_file(&args.csv_path).unwrap();

    let (mut handles_map, senders_map) = get_hashmaps(&headers);

    // read the csv line by line, and send the values to respective threads
    for line in file_reader.into_records(){
        let clean_line = line.expect("Failed to get line");

        // get index of the column name and the column name
        for (index, header) in headers.iter().enumerate(){
            // get value of a given column in a given line
            let value = clean_line.get(index).expect("Failed to get value from index");

            // try to convert the value to float, if succeeds, use Float type, if it throws an
            // error, use Text type
            let converted_value = match value.parse::<f64>(){
                Ok(v) => ColumnType::Float(v),
                Err(_) => ColumnType::Text(value.to_owned())
            };

            // send the value to the thread that manages this column
            match senders_map[header].send(converted_value){
                Ok(_) => {},
                Err(e) => {
                    println!("Skipped a row because of sending to thread problem: {}", e);
                }
            }
        }
    }

    // send a FileEnd message to every thread, so that they stop working
    for header in headers.iter(){
        match senders_map[header].send(ColumnType::FileEnd){
            Ok(_) => {},
            Err(e) => {
                println!("Did not end iter end messgage because of sending to thread problem: {}", e);
            }
        }
    }

    // prepare empty vectors for text and number column summaries
    let mut text_summary: Vec<(String, TextColumn)> = vec![];
    let mut number_summary: Vec<(String, NumberColumn)> = vec![];

    // joins all threads back into main, pushes the returned value to text or number summary vector
    for header in headers.iter(){
        let column_summary = handles_map
            .remove(header)
            .expect("Did not get a handle from header")
            .join()
            .expect("Counldn't join a handle");

        match column_summary{
            ColumnSummary::Text(v) => text_summary.push(
                (
                    header.to_owned(),
                    TextColumn{
                        categories: v.categories,
                        category_count: v.category_count
                    }
                )
            ),
            ColumnSummary::Number(v) => number_summary.push(
                (
                    header.to_owned(),
                    NumberColumn{
                        sum: v.sum,
                        mean: v.mean,
                        median: v.median,
                        std: v.std
                    }
                )
            )
        }
    }

    // displays all the results
    display_stats(text_summary, number_summary);
}
