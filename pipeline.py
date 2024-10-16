"""
Apache Beam pipeline to process property transactions data.

This pipeline reads a CSV file containing property transactions data, groups the transactions by property,
and creates a JSON object for each property. The JSON object contains the property's postcode, PAON, SAON,
street, town/city, district, county, and a list of transactions.

Author: Alex Smith
"""

import apache_beam as beam
import hashlib
import csv
import json
import os
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define the column headers with underscores instead of spaces
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

# Function to parse each line of the CSV into a list of values
def parse_csv_line(line):
    """
    Parse a line of the CSV file into a list of values.
    
    :param line: A string representing a line of the CSV file
    :return: A list of values representing a transaction
    """
    try:
        return next(csv.reader([line]))
    except Exception as e:
        logger.error(f"Error parsing line: {line} - {e}")
        return []

# Custom DoFn to group transactions by property (postcode, PAON, SAON)
class GroupTransactionsByProperty(beam.DoFn):
    def process(self, transaction):
        """
        Process a transaction and return a key-value pair of (postcode, PAON, SAON) key and list of transactions.

        :param transaction: A list of values representing a transaction
        :return: A list containing a single tuple of (key, transaction)
        """
        try:
            postcode = transaction[headers.index('Postcode')]
            paon = transaction[headers.index('PAON')]
            saon = transaction[headers.index('SAON')]
            key = (postcode, paon, saon)
            return [(key, transaction)]
        except Exception as e:
            logger.error(f"Error processing transaction: {transaction} - {e}")
            return []  # Yielding empty on error for safe processing

# Custom DoFn to create a JSON object from grouped transactions
class CreatePropertyObject(beam.DoFn):
    def process(self, key_value):
        """
        Process a key-value pair containing a list of transactions and return a single string containing a JSON object representing the property.
        
        :param key_value: A tuple containing a key (postcode, PAON, SAON) and a list of transactions
        :return: A list containing a single string representing a JSON object of the property
        """
        key, transactions = key_value
        postcode, paon, saon = key

        # Create a unique ID for the property
        property_id = hashlib.md5(f"{postcode}{paon}{saon}".encode()).hexdigest()

        try:
            # Extract consistent address details from the first transaction in the group
            street = transactions[0][headers.index('Street')]
            town_city = transactions[0][headers.index('Town_City')]
            district = transactions[0][headers.index('District')]
            county = transactions[0][headers.index('County')]

            # Sort the transactions by date
            transactions.sort(key=lambda x: datetime.strptime(x[headers.index('Date_of_Transfer')], '%Y-%m-%d %H:%M'))

            # Create the property object with all relevant details
            property_object = {
                'Property_ID': property_id,
                'Postcode': postcode,
                'PAON': paon,
                'SAON': saon,
                'Street': street,
                'Town_City': town_city,
                'District': district,
                'County': county,
                'Transactions': [
                    dict((k, v) for k, v in zip(headers, t) if k not in ['Postcode', 'PAON', 'SAON', 'Street', 'Town_City', 'District', 'County', 'Locality'])
                    for t in transactions
                ]  # !!!! Dropping Locality completely as it seems to be no longer a field used by GOV.UK
            }

            yield json.dumps(property_object, indent=4)  # Use yield for better memory management
        except Exception as e:
            logger.error(f"Error creating property object for key {key}: {e}")
            yield {}  # Yielding an empty dict on error

# Get current timestamp for output file path
timestamp = datetime.now().strftime("%Y-%m-%d_%H.%M.%S")
output_directory = f"Pipeline_Output/{timestamp}"

# Define the pipeline
with beam.Pipeline() as pipeline:
    data = (
        pipeline # CURRENTLY READING DATA WITH MORE THAN 1 TRANSACTION PER PROPERTY
        | 'Read CSV File' >> beam.io.ReadFromText('./filtered_rows_N12.csv', skip_header_lines=1)  # Reading CSV file, skipping the header line
        | 'Parse CSV Lines' >> beam.Map(parse_csv_line)  # Parse each line
        | 'Group Transactions' >> beam.ParDo(GroupTransactionsByProperty())  # Group transactions by property
        | 'Group By Key' >> beam.GroupByKey()  # Group transactions by (postcode, PAON, SAON) key
        | 'Create Property Object' >> beam.ParDo(CreatePropertyObject())  # Create JSON property objects
        | 'Filter Empty Results' >> beam.Filter(lambda x: x != {})  # Remove empty results due to errors
        | 'Write Output' >> beam.io.WriteToText(
            os.path.join(output_directory, 'Transactions_Per_Property'),
            file_name_suffix='.json',
            shard_name_template='-SSSSS-of-NNNNN',
            max_bytes_per_shard=512 * 1024 * 1024  # 512MB per shard for performance optimization
        )
    )

    # Log pipeline completion
    logger.info("Pipeline execution completed successfully.")
