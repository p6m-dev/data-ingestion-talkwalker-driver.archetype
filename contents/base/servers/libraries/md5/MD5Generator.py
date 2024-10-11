# Import necessary libraries
import json
import hashlib
import argparse


# Create a class for processing JSON data
class MD5Generator:
    def __init__(self, input_json, keys_to_exclude, delimiter):
        # Initialize the JSONProcessor with input JSON data, keys to exclude, and delimiter
        self.input_json = input_json
        self.keys_to_exclude = keys_to_exclude
        self.delimiter = delimiter

    def exclude_keys(self, json_data):
        # Recursive function to exclude specified keys from JSON data
        if isinstance(json_data, dict):
            return {
                key: self.exclude_keys(value)
                for key, value in json_data.items()
                if key not in self.keys_to_exclude
            }
        elif isinstance(json_data, list):
            return [self.exclude_keys(element) for element in json_data]
        else:
            return json_data

    def sort_keys_and_values(self, json_data):
        # Recursive function to sort keys and values in JSON data
        if isinstance(json_data, dict):
            return {
                key: self.sort_keys_and_values(value)
                for key, value in sorted(json_data.items())
            }
        elif isinstance(json_data, list):
            return [self.sort_keys_and_values(element) for element in json_data]
        else:
            return json_data

    def concatenate_keys_and_values(self, json_data):
        # Recursive function to concatenate keys and values in JSON data with a delimiter
        if isinstance(json_data, dict):
            return self.delimiter.join(
                f"{key}{self.delimiter}{self.concatenate_keys_and_values(value)}"
                for key, value in json_data.items()
            )
        elif isinstance(json_data, list):
            return self.delimiter.join(
                self.concatenate_keys_and_values(element) for element in json_data
            )
        else:
            return str(json_data)

    def process_json(self):
        # Step 1: Exclude specified keys from the input JSON
        excluded_json = self.exclude_keys(self.input_json)

        # Step 2: Sort keys and values in the JSON
        sorted_json = self.sort_keys_and_values(excluded_json)

        # Step 3: Concatenate keys and values with a delimiter
        concatenated_str = self.concatenate_keys_and_values(sorted_json)

        # Step 4: Remove spaces from the concatenated string
        trimmed_str = concatenated_str.replace(" ", "")

        # Step 5: Convert the trimmed string to lowercase
        lowercase_str = trimmed_str.lower()

        # Step 6: Generate MD5 hash (Query ID) from the lowercase string
        query_id = hashlib.md5(lowercase_str.encode()).hexdigest()

        # Return the processed JSON, concatenated string, trimmed string, lowercase string, and query ID
        return sorted_json, concatenated_str, trimmed_str, lowercase_str, query_id