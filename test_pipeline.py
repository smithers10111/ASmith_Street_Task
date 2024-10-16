import pytest
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
import hashlib
import json

# Sample headers used in the original pipeline
headers = [
    "Transaction_Unique_ID",
    "Price",
    "Date_of_Transfer",
    "Postcode",
    "Property_Type",
    "New_Build",
    "Duration",
    "PAON",
    "SAON",
    "Street",
    "Locality",
    "Town_City",
    "District",
    "County",
    "PPD_Category_Type",
    "Record_Status"
]

# Functions and DoFns from the original pipeline
from pipeline import parse_csv_line, GroupTransactionsByProperty, CreatePropertyObject

# Unit test for parsing a CSV line
def test_parse_csv_line():
    # A simple well-formed CSV line
    line = "123,250000,2022-01-15,N12 0NB,D"
    expected_output = ["123", "250000", "2022-01-15", "N12 0NB", "D"]
    
    assert parse_csv_line(line) == expected_output

def test_parse_csv_line_with_missing_fields():
    # A CSV line with missing fields
    line = "123,250000,2022-01-15,,"
    expected_output = ["123", "250000", "2022-01-15", "", ""]
    
    assert parse_csv_line(line) == expected_output

# Unit test for GroupTransactionsByProperty
def test_group_transactions_by_property():
    # Test a transaction with mock data
    transaction = [
        "123", "250000", "2022-01-15", "N12 0NB", "D", "0", "L", "45", "12", "", "Abbey Rd", "", "London", "Barnet", "London", "A"
    ]
    group = GroupTransactionsByProperty()
    result = list(group.process(transaction))
    
    # Expected key: (postcode, PAON, SAON)
    expected_key = ("N12 0NB", "45", "12")
    assert result == [(expected_key, transaction)]

# Unit test for CreatePropertyObject
def test_create_property_object():
    # Test grouped transactions with correct values as per the headers
    key = ("N12 0NB", "45", "12")
    transactions = [
        [
            "123", "250000", "2022-01-15 12:30", "N12 0NB", "D", "0", "L", 
            "45", "12", "Abbey Rd", "", "London", "Barnet", "London", "A", "A"
        ],
        [
            "456", "300000", "2022-02-01 15:45", "N12 0NB", "D", "0", "L", 
            "45", "12", "Abbey Rd", "", "London", "Barnet", "London", "A", "A"
        ]
    ]
    
    # Create the property object instance
    property_object = CreatePropertyObject()
    result = list(property_object.process((key, transactions)))

    # Generate the expected Property_ID
    property_id = hashlib.md5(f"{key[0]}{key[1]}{key[2]}".encode()).hexdigest()

    # Expected output structure based on headers
    expected_output = {
        'Property_ID': property_id,
        'Postcode': "N12 0NB",
        'PAON': "45",
        'SAON': "12",
        'Street': "Abbey Rd",
        'Town_City': "London",
        'District': "Barnet",
        'County': "London",
        'Transactions': [
            {
                "Transaction_Unique_ID": "123",
                "Price": "250000",
                "Date_of_Transfer": "2022-01-15 12:30",
                "Property_Type": "D",
                "New_Build": "0",
                "Duration": "L",
                "PPD_Category_Type": "A",
                "Record_Status": "A"
            },
            {
                "Transaction_Unique_ID": "456",
                "Price": "300000",
                "Date_of_Transfer": "2022-02-01 15:45",
                "Property_Type": "D",
                "New_Build": "0",
                "Duration": "L",
                "PPD_Category_Type": "A",
                "Record_Status": "A"
            }
        ]
    }
    
    # Load the result into a Python dictionary and compare
    assert json.loads(result[0]) == expected_output

# Integration test for the full pipeline
def test_full_pipeline():
    # Input data that matches the header structure
    input_data = [
        "123,250000,2022-01-15 12:30,N12 0NB,D,0,L,45,12,Abbey Rd,,London,Barnet,London,A,A",
        "456,300000,2022-02-01 15:45,N12 0NB,D,0,L,45,12,Abbey Rd,,London,Barnet,London,A,A"
    ]
    
    # Expected output with fields aligned to headers
    expected_output = [
        json.dumps({
            'Property_ID': hashlib.md5("N12 0NB4512".encode()).hexdigest(),
            'Postcode': "N12 0NB",
            'PAON': "45",
            'SAON': "12",
            'Street': "Abbey Rd",
            'Town_City': "London",
            'District': "Barnet",
            'County': "London",
            'Transactions': [
                {
                    "Transaction_Unique_ID": "123",
                    "Price": "250000",
                    "Date_of_Transfer": "2022-01-15 12:30",
                    "Property_Type": "D",
                    "New_Build": "0",
                    "Duration": "L",
                    "PPD_Category_Type": "A",
                    "Record_Status": "A"
                },
                {
                    "Transaction_Unique_ID": "456",
                    "Price": "300000",
                    "Date_of_Transfer": "2022-02-01 15:45",
                    "Property_Type": "D",
                    "New_Build": "0",
                    "Duration": "L",
                    "PPD_Category_Type": "A",
                    "Record_Status": "A"
                }
            ]
        }, indent=4)
    ]
    
    # Simulating the pipeline
    with beam.Pipeline() as p:
        input_pcoll = (
            p
            | 'Create Input' >> beam.Create(input_data)  # Simulating input data
            | 'Parse CSV Lines' >> beam.Map(parse_csv_line)  # Parse CSV
            | 'Group Transactions' >> beam.ParDo(GroupTransactionsByProperty())  # Group transactions
            | 'Group By Key' >> beam.GroupByKey()  # Group by (postcode, PAON, SAON)
            | 'Create Property Object' >> beam.ParDo(CreatePropertyObject())  # Create property JSON object
        )
        
        # Assert that the output matches the expected output
        beam.testing.util.assert_that(
            input_pcoll,
            beam.testing.util.equal_to(expected_output)
        )


def test_create_property_object_with_single_transaction_per_address():
    # Test grouped transactions where each property has only one transaction
    key1 = ("N12 0NB", "45", "12")
    transactions1 = [
        ["123", "250000", "2022-01-15 12:30", "N12 0NB", "D", "0", "L", "45", "12", "", "", "London", "Barnet", "London", "A", "A"]
    ]
    
    key2 = ("N13 1AB", "10", "5")
    transactions2 = [
        ["789", "450000", "2022-03-10 10:15", "N13 1AB", "D", "0", "L", "10", "5", "", "", "London", "Islington", "London", "A", "A"]
    ]

    property_object = CreatePropertyObject()

    # Processing each property and getting results
    result1 = list(property_object.process((key1, transactions1)))
    result2 = list(property_object.process((key2, transactions2)))

    # Calculate Property IDs for expected outputs
    property_id1 = hashlib.md5(f"{key1[0]}{key1[1]}{key1[2]}".encode()).hexdigest()
    property_id2 = hashlib.md5(f"{key2[0]}{key2[1]}{key2[2]}".encode()).hexdigest()

    # Create expected outputs
    expected_output1 = {
        'Property_ID': property_id1,
        'Postcode': "N12 0NB",
        'PAON': "45",
        'SAON': "12",
        'Street': "",
        'Town_City': "London",
        'District': "Barnet",
        'County': "London",
        'Transactions': [
            {
                "Transaction_Unique_ID": "123",
                "Price": "250000",
                "Date_of_Transfer": "2022-01-15 12:30",
                "Property_Type": "D",
                "New_Build": "0",
                "Duration": "L",
                "PPD_Category_Type": "A",
                "Record_Status": "A"
            }
        ]
    }

    expected_output2 = {
        'Property_ID': property_id2,
        'Postcode': "N13 1AB",
        'PAON': "10",
        'SAON': "5",
        'Street': "",
        'Town_City': "London",
        'District': "Islington",
        'County': "London",
        'Transactions': [
            {
                "Transaction_Unique_ID": "789",
                "Price": "450000",
                "Date_of_Transfer": "2022-03-10 10:15",
                "Property_Type": "D",
                "New_Build": "0",
                "Duration": "L",
                "PPD_Category_Type": "A",
                "Record_Status": "A"
            }
        ]
    }

    assert json.loads(result1[0]) == expected_output1
    assert json.loads(result2[0]) == expected_output2
