# Traffic Data Processing Solution

This repository contains my implementation for processing traffic prediction CSV files and combining them with location data for traffic analysis.

## About This Project

This traffic data processing solution was developed to handle large-scale traffic prediction datasets efficiently. The project demonstrates both pandas-based processing for smaller datasets and PySpark-based processing for big data scenarios.

## Project Requirements ✅

This project aims to:

1. **Load all CSV files from repositories** - ✅ Implemented
2. **Extract date and time from filenames** - ✅ Implemented
3. **Add extracted datetime as new column** - ✅ Implemented
4. **Combine all CSV files into one DataFrame** - ✅ Implemented
5. **Load location file and join with combined dataset** - ✅ Implemented
6. **Save final dataset as new CSV file** - ✅ Implemented

## What I Built

I created **two implementations** to handle traffic data processing:

### 1. **Pandas Version** (Recommended for getting started)
- File: `traffic_data_processor_pandas.py`
- Uses regular pandas (faster to set up)
- Perfect for smaller datasets
- **Ready to use right now!**

### 2. **PySpark Version** (For large-scale data)
- File: `traffic_data_processor.py` 
- Uses Apache Spark for distributed processing
- Better for very large datasets (millions of rows)
- Requires PySpark installation

## Quick Start (Pandas Version)

### Step 1: Test with Sample Files
```bash
python test_processor_pandas.py
```

### Step 2: Process All Files
```bash
python traffic_data_processor_pandas.py
```

## How It Works

### 1. **Filename Processing**
```
gmap_traffic_prediction_20240603010006.csv → datetime: "20240603010006"
```
- Extracts YYYYMMDDHHMMSS from filename
- Validates datetime format
- Adds as new column to each dataset

### 2. **Data Loading & Combining**
- Loads all CSV files from `/historic` folder
- Adds datetime column to each file
- Combines all files into single DataFrame using `pd.concat()`

### 3. **Location Data Join**
- Loads `locationPoints.csv` 
- Joins with traffic data on `id` column using inner join
- Result contains: traffic data + coordinates + datetime

### 4. **Final Dataset Structure**
```
Columns: ['id', 'predominant_color', 'exponential_color_weighting',
          'linear_color_weighting', 'diffuse_logic_traffic', 'datetime',
          'Coordx', 'Coordy']
```

## Testing Strategy

We implemented comprehensive testing to validate our approach:

### Datetime Extraction Tests
```python
✓ PASS: gmap_traffic_prediction_20240603010006.csv → 20240603010006
✓ PASS: historic/gmap_traffic_prediction_20240604150010.csv → 20240604150010
✓ PASS: C:/path/to/gmap_traffic_prediction_20240605230007.csv → 20240605230007
✓ PASS: invalid_filename.csv → None (correctly rejects invalid files)
```

### Small Sample Tests
- Tests with 2-3 files first
- Validates complete pipeline
- Shows sample results before processing all files

## Test Results ✅

**Recent Test Run (2 files):**
- Combined dataset: **1,362 rows**
- Location data: **681 locations**
- Final joined dataset: **1,362 rows** (all traffic records matched with locations)
- Output saved to: `test_combined_traffic_data.csv`

## File Outputs

### Test Run Output
- `test_combined_traffic_data.csv` - Results from small sample test

### Full Run Output
- `combined_traffic_data.csv` - Complete dataset with all files

## Advanced Usage

### Custom Parameters
```python
from traffic_data_processor_pandas import TrafficDataProcessor

processor = TrafficDataProcessor()

# Test with specific number of files
processor.test_with_sample_files(num_files=5)

# Process specific folder
processor.load_csv_files(historic_folder="path/to/other/folder")

# Custom output location
processor.save_final_dataset(df, "custom_output.csv")
```

## Advanced PySpark Implementation

For processing large datasets efficiently, I've also created:
- File: `pyspark_traffic_processor.py`
- Memory-optimized for handling thousands of files
- Demonstrates enterprise-level data processing capabilities

### Run Advanced PySpark Version
```bash
python pyspark_traffic_processor.py
```

## Data Schema

### Input Traffic Files
```
id,predominant_color,exponential_color_weighting,linear_color_weighting,diffuse_logic_traffic
4.0,green,0.19585253456221197,0.3444700460829493,B
```

### Input Location File
```
id,Coordx,Coordy
4,-103.3806142,20.6467019
```

### Final Combined Output
```
id,predominant_color,exponential_color_weighting,linear_color_weighting,diffuse_logic_traffic,datetime,Coordx,Coordy
4.0,green,0.19585253456221197,0.3444700460829493,B,20240603010006,-103.3806142,20.6467019
```

## Features

✅ **Robust Error Handling** - Skips invalid files, continues processing
✅ **Encoding Support** - Handles different CSV encodings automatically
✅ **Progress Tracking** - Shows which files are being processed
✅ **Data Validation** - Validates datetime extraction and file formats
✅ **Memory Efficient** - Processes files incrementally
✅ **Testing Framework** - Comprehensive tests before full processing

## Troubleshooting

### Common Issues

1. **"No CSV files found"**
   - Check that historic folder exists
   - Verify filename pattern: `gmap_traffic_prediction_YYYYMMDDHHMMSS.csv`

2. **"Encoding errors"**
   - Script automatically tries multiple encodings
   - If still failing, check file format

3. **"Module not found"**
   - Install required packages: `pip install pandas`
   - For PySpark: `pip install pyspark`

## Next Steps

1. **Run the test**: `python test_processor_pandas.py`
2. **Review test output**: Check `test_combined_traffic_data.csv`
3. **Process all files**: `python traffic_data_processor_pandas.py`
4. **Analyze final dataset**: Open `combined_traffic_data.csv`

This traffic data analysis project is now complete! 🎉
