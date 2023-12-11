import sys
sys.path.append('etl.py')
from ETL import extract, transform, load

def test_extract():
    # Setup any necessary inputs for the extract function
    # Call the extract function
    result = extract()
    # Assert that the result is as expected
    assert isinstance(result, str), "Expected result to be a string"
    # Add more assertions based on what extract() should return

def test_transform():
    # Setup any necessary inputs for the transform function
    processed_file_name = 'IngestionFiles/ConvertedRawData.json'
    # Call the transform function
    result = transform(processed_file_name)
    # Assert that the result is as expected
    assert isinstance(result, list)  # Example assertion
    # Add more assertions based on what transform() should return

def main():
    test_extract()
    test_transform()

if __name__ == '__main__':
    main()