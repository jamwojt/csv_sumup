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
    pub fn new() -> Self {
        return TextColumn {
            categories: HashSet::new(),
            category_count: 0,
        };
    }

    pub fn add_to_categories(&mut self, value: String) {
        if !self.categories.contains(&value) {
            self.categories.insert(value);
            self.category_count += 1;
        }
    }

    pub fn build_summary(&self) -> TextColumn {
        let mut text_column_summary = TextColumn::new();
        text_column_summary.set_categories(self.categories.clone());
        text_column_summary.set_category_count(self.category_count);

        return text_column_summary;
    }

    pub fn set_categories(&mut self, categories: HashSet<String>) {
        self.categories = categories;
    }

    pub fn set_category_count(&mut self, category_count: u16) {
        self.category_count = category_count;
    }

    pub fn get_categories(&self) -> HashSet<String> {
        return self.categories.clone();
    }

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
    pub fn new() -> Self {
        return NumberColumn {
            sum: 0.0,
            mean: 0.0,
            median: 0.0,
            std: 0.0,
        };
    }

    pub fn build_summary(&self) -> NumberColumn{
        let mut number_column_summary = NumberColumn::new();
        number_column_summary.set_sum(self.get_sum());
        number_column_summary.set_mean(self.get_mean());
        number_column_summary.set_median(self.get_median());
        number_column_summary.set_std(self.get_std());

        return number_column_summary;
    }

    pub fn get_sum(&self) -> f64 {
        return self.sum;
    }

    pub fn get_mean(&self) -> f64 {
        return self.mean;
    }

    pub fn get_median(&self) -> f64 {
        return self.median;
    }

    pub fn get_std(&self) -> f64 {
        return self.std;
    }

    pub fn set_sum(&mut self, sum: f64) {
        self.sum = sum;
    }

    pub fn set_mean(&mut self, mean: f64) {
        self.mean = mean;
    }

    pub fn set_median(&mut self, median: f64) {
        self.median = median;
    }

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
    pub fn new() -> Self {
        return DateColumn {
            earliest: None,
            latest: None,
        };
    }

    pub fn build_summary(&self) -> DateColumn{
        let mut date_column_summary = DateColumn::new();
        date_column_summary.set_earliest(self.get_earliest().unwrap());
        date_column_summary.set_latest(self.get_latest().unwrap());

        return date_column_summary;
    }

    pub fn get_earliest(&self) -> Option<NaiveDate> {
        return self.earliest;
    }

    pub fn get_latest(&self) -> Option<NaiveDate> {
        return self.latest;
    }

    pub fn set_earliest(&mut self, date: NaiveDate) {
        self.earliest = Some(date);
    }

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
    pub fn new() -> Self {
        return PossibleDate {
            year: None,
            month: None,
            day: None,
        };
    }

    pub fn get_year(&self) -> Option<i32> {
        return self.year;
    }

    pub fn get_month(&self) -> Option<u32> {
        return self.month;
    }

    pub fn get_day(&self) -> Option<u32> {
        return self.day;
    }

    pub fn set_year(&mut self, year: i32) {
        self.year = Some(year);
    }

    pub fn set_month(&mut self, month: u32) {
        self.month = Some(month);
    }

    pub fn set_day(&mut self, day: u32) {
        self.day = Some(day);
    }
}
