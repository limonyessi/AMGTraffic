"""
Windows-Compatible PySpark Test Script
This version processes with PySpark but saves with pandas to avoid Windows/Hadoop issues
"""

from traffic_data_processor import TrafficDataProcessor
import sys

def test_datetime_extraction():
    """Test the datetime extraction function"""
    processor = TrafficDataProcessor()
    
    # Test cases
    test_files = [
        "gmap_traffic_prediction_20240603010006.csv",
        "historic/gmap_traffic_prediction_20240604150010.csv",
        "C:/path/to/gmap_traffic_prediction_20240605230007.csv",
        "invalid_filename.csv",
        "gmap_traffic_prediction_2024060301000.csv"  # Invalid - wrong length
    ]
    
    expected_results = [
        "20240603010006",
        "20240604150010", 
        "20240605230007",
        None,
        None
    ]
    
    print("Testing datetime extraction function...")
    for i, test_file in enumerate(test_files):
        result = processor.extract_datetime_from_filename(test_file)
        expected = expected_results[i]
        
        if result == expected:
            print(f"✓ PASS: {test_file} -> {result}")
        else:
            print(f"✗ FAIL: {test_file} -> Expected: {expected}, Got: {result}")
    
    processor.stop()

def test_small_sample():
    """Test with a small sample of files"""
    print("\n" + "="*50)
    print("Testing with small sample of files (Windows-compatible)...")
    print("="*50)
    
    processor = TrafficDataProcessor()
    
    try:
        # Load sample traffic data
        traffic_df = processor.load_csv_files(limit_files=2)
        
        # Load location data
        location_df = processor.load_location_data()
        
        # Join the data
        final_df = processor.join_with_location_data(traffic_df, location_df)
        
        # Convert to pandas and save (Windows-compatible)
        print("\nConverting to pandas for Windows-compatible saving...")
        pandas_df = final_df.toPandas()
        
        # Save test results using pandas
        test_output_path = r"c:\Users\User\Documents\GitHub\AMGTraffic\test_combined_traffic_data_pyspark.csv"
        pandas_df.to_csv(test_output_path, index=False)
        
        print(f"\n✅ SUCCESS! Final dataset saved to: {test_output_path}")
        print(f"Dataset contains {len(pandas_df)} rows and {len(pandas_df.columns)} columns")
        print("Columns:", list(pandas_df.columns))
        
        # Show sample of final data
        print("\nSample of final data (first 5 rows):")
        print(pandas_df.head())
        
        processor.stop()
        return True
        
    except Exception as e:
        print(f"✗ Test failed: {str(e)}")
        processor.stop()
        return False

def main():
    """Run all tests"""
    print("Starting PySpark Traffic Data Processor Tests (Windows-Compatible)...")
    print("="*70)
    
    # Test datetime extraction
    test_datetime_extraction()
    
    # Test with small sample
    success = test_small_sample()
    
    if success:
        print("\n" + "="*70)
        print("🎉 ALL TESTS PASSED! PySpark processing completed successfully!")
        print("Your traffic analysis project is ready with PySpark implementation!")
        print("="*70)
        print("\nNext steps:")
        print("1. Check the output file: test_combined_traffic_data_pyspark.csv")
        print("2. To process ALL files, run: python traffic_data_processor_windows.py")
    else:
        print("\n" + "="*70)
        print("❌ Some tests failed. Please check the output above.")
        print("="*70)
        sys.exit(1)

if __name__ == "__main__":
    main()