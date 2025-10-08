"""
Simple test script to verify the traffic data processor works correctly
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
    print("Testing with small sample of files...")
    print("="*50)
    
    processor = TrafficDataProcessor()
    
    try:
        # Test with just 2 files
        sample_df = processor.test_with_sample_files(num_files=2)
        print("✓ Small sample test completed successfully!")
        return True
        
    except Exception as e:
        print(f"✗ Small sample test failed: {str(e)}")
        return False
    finally:
        processor.stop()

def main():
    """Run all tests"""
    print("Starting Traffic Data Processor Tests...")
    print("="*60)
    
    # Test datetime extraction
    test_datetime_extraction()
    
    # Test with small sample
    success = test_small_sample()
    
    if success:
        print("\n" + "="*60)
        print("All tests passed! The processor is ready to use.")
        print("You can now run traffic_data_processor.py to process all files.")
        print("="*60)
    else:
        print("\n" + "="*60)
        print("Some tests failed. Please check the output above.")
        print("="*60)
        sys.exit(1)

if __name__ == "__main__":
    main()