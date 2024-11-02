//! stores encapsulation objects that organize summary values for different column types.

use chrono::NaiveDate;
use std::collections::HashSet;

// stores value from one field
pub enum ColumnType {
    Text(String),
    Float(f64),
    Date(NaiveDate),
    FileEnd,
}

// stores summary of a column with text values
pub struct TextColumn {
    categories: HashSet<String>,
    category_count: u16,
}

impl TextColumn {
    /// returns new TextColumn object with empty categories set and category_count set to 0
    pub fn new() -> Self {
        return TextColumn {
            categories: HashSet::new(),
            category_count: 0,
        };
    }

    /// method that checks is a value is already in categories vector.
    /// if not, it adds the passed value to categories HashSet and increases category_count by 1.
    pub fn add_to_categories(&mut self, value: String) {
        if !self.categories.contains(&value) {
            self.categories.insert(value);
            self.category_count += 1;
        }
    }

    /// method that returns a new TextColumn object from an existing one with the same values. Mostly used to get a
    /// TextColumn object out of other encapsulations
    pub fn build_summary(&self) -> TextColumn {
        let mut text_column_summary = TextColumn::new();
        text_column_summary.set_categories(self.categories.clone());
        text_column_summary.set_category_count(self.category_count);

        return text_column_summary;
    }

    /// method that sets categories field to a HashSet passed in this method.
    pub fn set_categories(&mut self, categories: HashSet<String>) {
        self.categories = categories;
    }

    /// method that sets category count to a number passed in this method.
    pub fn set_category_count(&mut self, category_count: u16) {
        self.category_count = category_count;
    }

    /// method that returns categories field using clone().
    pub fn get_categories(&self) -> HashSet<String> {
        return self.categories.clone();
    }

    /// method that returns category_count from a TextColumn object.
    pub fn get_category_count(&self) -> u16 {
        return self.category_count;
    }
}

// stores summary of a column with number values
pub struct NumberColumn {
    sum: f64,
    mean: f64,
    median: f64,
    std: f64,
}

impl NumberColumn {
    /// returns a new NumberColumn object with all values set to 0.0
    pub fn new() -> Self {
        return NumberColumn {
            sum: 0.0,
            mean: 0.0,
            median: 0.0,
            std: 0.0,
        };
    }

    /// method that returns a NumberColumn object from an existing one. Mostly used to get a
    /// NumberColumn object out of other encapsulation
    pub fn build_summary(&self) -> NumberColumn {
        let mut number_column_summary = NumberColumn::new();
        number_column_summary.set_sum(self.get_sum());
        number_column_summary.set_mean(self.get_mean());
        number_column_summary.set_median(self.get_median());
        number_column_summary.set_std(self.get_std());

        return number_column_summary;
    }

    /// returns sum field from the object
    pub fn get_sum(&self) -> f64 {
        return self.sum;
    }

    /// returns mean field from the object
    pub fn get_mean(&self) -> f64 {
        return self.mean;
    }

    /// returns median field from the object
    pub fn get_median(&self) -> f64 {
        return self.median;
    }

    /// returns standard deviation field from the object
    pub fn get_std(&self) -> f64 {
        return self.std;
    }

    /// sets the sum field
    pub fn set_sum(&mut self, sum: f64) {
        self.sum = sum;
    }

    /// sets the mean field
    pub fn set_mean(&mut self, mean: f64) {
        self.mean = mean;
    }

    /// sets the median field
    pub fn set_median(&mut self, median: f64) {
        self.median = median;
    }

    /// sets the standard deviation field
    pub fn set_std(&mut self, std: f64) {
        self.std = std;
    }
}

// stores summary of a column with date values
pub struct DateColumn {
    earliest: Option<NaiveDate>,
    latest: Option<NaiveDate>,
}

impl DateColumn {
    /// creates new DateColumn object with both earliest and latest fields set to None
    pub fn new() -> Self {
        return DateColumn {
            earliest: None,
            latest: None,
        };
    }

    /// returns a new DateColumn object from an existing one. Mostly used to extract DateColumn
    /// objects from other encapsulations
    pub fn build_summary(&self) -> DateColumn {
        let mut date_column_summary = DateColumn::new();
        date_column_summary.set_earliest(self.get_earliest().unwrap());
        date_column_summary.set_latest(self.get_latest().unwrap());

        return date_column_summary;
    }

    /// returns earliest field from the object
    pub fn get_earliest(&self) -> Option<NaiveDate> {
        return self.earliest;
    }

    /// returns latest field from the object
    pub fn get_latest(&self) -> Option<NaiveDate> {
        return self.latest;
    }

    /// sets earliest field of the object to Some(passed_value) where passed_value is of type
    /// chrono::NaiveDate
    pub fn set_earliest(&mut self, date: NaiveDate) {
        self.earliest = Some(date);
    }

    /// sets latest fields of the object to Some(passed_value) where passed_value is of type
    /// chrono::NaiveDate
    pub fn set_latest(&mut self, date: NaiveDate) {
        self.latest = Some(date);
    }
}

// stores summary of any column
pub enum ColumnSummary {
    Text(TextColumn),
    Number(NumberColumn),
    Date(DateColumn),
}

pub struct PossibleDate {
    year: Option<i32>,
    month: Option<u32>,
    day: Option<u32>,
}

impl PossibleDate {
    /// returns a new PossibleDate object with year, month, and day values set to None
    pub fn new() -> Self {
        return PossibleDate {
            year: None,
            month: None,
            day: None,
        };
    }

    /// returns the year value from the object
    pub fn get_year(&self) -> Option<i32> {
        return self.year;
    }

    /// returns the month value from the object
    pub fn get_month(&self) -> Option<u32> {
        return self.month;
    }

    /// returns the day value from the object
    pub fn get_day(&self) -> Option<u32> {
        return self.day;
    }

    /// sets the year field to Some(passed_value)
    pub fn set_year(&mut self, year: i32) {
        self.year = Some(year);
    }

    /// sets the month field to Some(passed_value)
    pub fn set_month(&mut self, month: u32) {
        self.month = Some(month);
    }

    /// sets the day field to Some(passed_value)
    pub fn set_day(&mut self, day: u32) {
        self.day = Some(day);
    }
}
