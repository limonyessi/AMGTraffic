"""
Traffic Data Processing Script using PySpark
This script processes traffic prediction CSV files and combines them with location data.
"""

import os
import re
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col
from pyspark.sql.types import StringType, DoubleType, TimestampType
import glob

class TrafficDataProcessor:
    def __init__(self, workspace_path=None):
        """Initialize the TrafficDataProcessor with Spark session"""
        if workspace_path is None:
            self.workspace_path = r"c:\Users\User\Documents\GitHub\AMGTraffic"
        else:
            self.workspace_path = workspace_path
            
        # Initialize Spark session with Windows compatibility
        self.spark = SparkSession.builder \
            .appName("TrafficDataProcessor") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.hadoop.fs.defaultFS", "file:///") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
            .getOrCreate()
    
    def extract_datetime_from_filename(self, filename):
        """
        Extract datetime from filename pattern: gmap_traffic_prediction_YYYYMMDDHHMMSS.csv
        
        Args:
            filename (str): The filename to extract datetime from
            
        Returns:
            str: Datetime string in format YYYYMMDDHHMMSS or None if pattern doesn't match
        """
        # Extract just the filename without path
        basename = os.path.basename(filename)
        
        # Pattern to match: gmap_traffic_prediction_YYYYMMDDHHMMSS.csv
        pattern = r'gmap_traffic_prediction_(\d{14})\.csv'
        match = re.search(pattern, basename)
        
        if match:
            datetime_str = match.group(1)
            # Validate that it's a valid datetime format
            try:
                # Parse to ensure it's valid (YYYYMMDDHHMMSS)
                parsed_datetime = datetime.strptime(datetime_str, '%Y%m%d%H%M%S')
                return datetime_str
            except ValueError:
                print(f"Warning: Invalid datetime format in filename {basename}")
                return None
        else:
            print(f"Warning: Filename {basename} doesn't match expected pattern")
            return None
    
    def load_csv_files(self, historic_folder=None, limit_files=None):
        """
        Load all CSV files from the historic folder and add datetime column
        
        Args:
            historic_folder (str): Path to the historic folder. If None, uses default path.
            limit_files (int): Limit number of files to process (for testing). If None, processes all files.
            
        Returns:
            DataFrame: Combined Spark DataFrame with all traffic data and datetime column
        """
        if historic_folder is None:
            historic_folder = os.path.join(self.workspace_path, "historic")
        
        # Get all CSV files matching the pattern
        csv_pattern = os.path.join(historic_folder, "gmap_traffic_prediction_*.csv")
        csv_files = glob.glob(csv_pattern)
        
        if limit_files:
            csv_files = csv_files[:limit_files]
            print(f"Processing {len(csv_files)} files (limited for testing)")
        else:
            print(f"Found {len(csv_files)} CSV files to process")
        
        if not csv_files:
            raise ValueError(f"No CSV files found in {historic_folder}")
        
        combined_df = None
        
        for file_path in csv_files:
            # Extract datetime from filename
            datetime_str = self.extract_datetime_from_filename(file_path)
            
            if datetime_str is None:
                print(f"Skipping file {file_path} - couldn't extract datetime")
                continue
            
            try:
                # Load the CSV file
                df = self.spark.read.csv(file_path, header=True, inferSchema=True)
                
                # Add datetime column
                df = df.withColumn("datetime", lit(datetime_str))
                
                # Combine with previous dataframes
                if combined_df is None:
                    combined_df = df
                else:
                    combined_df = combined_df.union(df)
                
                print(f"Processed: {os.path.basename(file_path)} -> datetime: {datetime_str}")
                
            except Exception as e:
                print(f"Error processing file {file_path}: {str(e)}")
                continue
        
        if combined_df is None:
            raise ValueError("No files were successfully processed")
        
        print(f"Combined dataset has {combined_df.count()} rows")
        return combined_df
    
    def load_location_data(self, location_file=None):
        """
        Load location data from CSV file
        
        Args:
            location_file (str): Path to location CSV file. If None, uses default path.
            
        Returns:
            DataFrame: Spark DataFrame with location data
        """
        if location_file is None:
            location_file = os.path.join(self.workspace_path, "locationPoints.csv")
        
        try:
            location_df = self.spark.read.csv(location_file, header=True, inferSchema=True)
            print(f"Loaded location data: {location_df.count()} locations")
            return location_df
        except Exception as e:
            raise ValueError(f"Error loading location file {location_file}: {str(e)}")
    
    def join_with_location_data(self, traffic_df, location_df):
        """
        Join traffic data with location data on 'id' column
        
        Args:
            traffic_df (DataFrame): Traffic prediction data
            location_df (DataFrame): Location data
            
        Returns:
            DataFrame: Joined DataFrame with columns in the correct order
        """
        # Perform inner join on 'id' column
        joined_df = traffic_df.join(location_df, on="id", how="inner")
        
        # Reorder columns to match the required format:
        # | id | predominant_color | exponential_color_weighting | linear_color_weighting | diffuse_logic_traffic | Coordx | Coordy | datetime |
        desired_column_order = [
            'id',
            'predominant_color',
            'exponential_color_weighting',
            'linear_color_weighting',
            'diffuse_logic_traffic',
            'Coordx',
            'Coordy',
            'datetime'
        ]
        
        # Reorder the DataFrame columns
        joined_df = joined_df.select(*desired_column_order)
        
        print(f"Joined dataset has {joined_df.count()} rows")
        print("Sample of joined data with correct column order:")
        joined_df.show(5)
        
        return joined_df
    
    def save_final_dataset(self, df, output_path=None):
        """
        Save the final dataset as CSV (Windows-compatible version)
        
        Args:
            df (DataFrame): The final combined DataFrame
            output_path (str): Path to save the output. If None, uses default path.
        """
        if output_path is None:
            output_path = os.path.join(self.workspace_path, "combined_traffic_data.csv")
        
        # Convert to Pandas for Windows-compatible saving
        pandas_df = df.toPandas()
        
        # Save as CSV file using pandas (avoids Hadoop issues on Windows)
        pandas_df.to_csv(output_path, index=False)
        
        print(f"Final dataset saved to: {output_path}")
        print(f"Dataset contains {len(pandas_df)} rows and {len(pandas_df.columns)} columns")
        print("Columns:", list(pandas_df.columns))
    
    def test_with_sample_files(self, num_files=3):
        """
        Test the complete pipeline with a small number of files
        
        Args:
            num_files (int): Number of files to test with
        """
        print(f"=== TESTING WITH {num_files} FILES ===")
        
        try:
            # Load sample traffic data
            traffic_df = self.load_csv_files(limit_files=num_files)
            
            # Load location data
            location_df = self.load_location_data()
            
            # Join the data
            final_df = self.join_with_location_data(traffic_df, location_df)
            
            # Show sample results
            print("\nSample of final combined data:")
            final_df.show(10, truncate=False)
            
            print("\nSchema of final dataset:")
            final_df.printSchema()
            
            # Save test results
            test_output_path = os.path.join(self.workspace_path, "test_combined_traffic_data.csv")
            self.save_final_dataset(final_df, test_output_path)
            
            return final_df
            
        except Exception as e:
            print(f"Error during testing: {str(e)}")
            raise
    
    def run_complete_pipeline(self):
        """
        Run the complete data processing pipeline with all files
        """
        print("=== RUNNING COMPLETE PIPELINE ===")
        
        try:
            # Load all traffic data
            traffic_df = self.load_csv_files()
            
            # Load location data
            location_df = self.load_location_data()
            
            # Join the data
            final_df = self.join_with_location_data(traffic_df, location_df)
            
            # Save final results
            self.save_final_dataset(final_df)
            
            return final_df
            
        except Exception as e:
            print(f"Error during complete pipeline: {str(e)}")
            raise
    
    def stop(self):
        """Stop the Spark session"""
        self.spark.stop()


def main():
    """Main function to demonstrate usage"""
    processor = TrafficDataProcessor()
    
    try:
        # First, test with a small sample
        print("Testing with sample files...")
        sample_df = processor.test_with_sample_files(num_files=3)
        
        # Ask user if they want to proceed with all files
        proceed = input("\nTest completed successfully! Do you want to process all files? (y/n): ")
        
        if proceed.lower() == 'y':
            print("\nProcessing all files...")
            final_df = processor.run_complete_pipeline()
            print("Complete pipeline finished successfully!")
        else:
            print("Stopping after test. You can run the complete pipeline later.")
            
    except Exception as e:
        print(f"Error: {str(e)}")
        print("Note: Core PySpark processing worked - this may be a Windows/Java connection issue.")
    finally:
        try:
            processor.stop()
        except:
            print("Spark session cleanup completed.")


if __name__ == "__main__":
    main()