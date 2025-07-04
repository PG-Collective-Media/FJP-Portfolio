#!/usr/bin/env python3
"""
CSV to T-SQL Converter Script

This script recursively scans a directory for CSV files, reads their column headers,
and generates T-SQL INSERT statements based on the actual data structure found in each file.

Usage:
    python csv_to_tsql.py [directory_path] [--debug]

Example:
    python csv_to_tsql.py ./csv_files --debug
"""

import os
import re
import argparse
import logging
import csv
from pathlib import Path
from typing import List, Dict, Tuple, Optional


class CSVToTSQLConverter:
    def __init__(self, debug: bool = False):
        self.debug = debug
        self.setup_logging()
        self.output_dir = Path("output_sql")
        self.ensure_output_directory()
    
    def setup_logging(self):
        """Setup logging configuration based on debug mode."""
        level = logging.DEBUG if self.debug else logging.INFO
        logging.basicConfig(
            level=level,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler('csv_to_tsql.log')
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def ensure_output_directory(self):
        """Create output directory if it doesn't exist."""
        self.output_dir.mkdir(exist_ok=True)
        self.logger.info(f"Output directory ensured: {self.output_dir}")
    
    def find_csv_files(self, directory: str) -> List[Path]:
        """Recursively find all CSV files in the given directory."""
        csv_files = []
        directory_path = Path(directory)
        
        if not directory_path.exists():
            self.logger.error(f"Directory does not exist: {directory}")
            return csv_files
        
        self.logger.info(f"Scanning directory: {directory}")
        
        for root, dirs, files in os.walk(directory):
            for file in files:
                if file.lower().endswith('.csv'):
                    csv_file = Path(root) / file
                    csv_files.append(csv_file)
                    self.logger.debug(f"CSV file found: {csv_file}")
        
        self.logger.info(f"Total CSV files found: {len(csv_files)}")
        return csv_files
    
    def derive_table_name(self, csv_file: Path) -> str:
        """Derive table name from CSV filename."""
        # Remove .csv extension and clean up the name
        table_name = csv_file.stem
        # Replace hyphens and other special characters with underscores
        table_name = re.sub(r'[^a-zA-Z0-9_]', '_', table_name)
        # Ensure it starts with a letter or underscore
        if table_name and not table_name[0].isalpha() and table_name[0] != '_':
            table_name = f"table_{table_name}"
        # Ensure it's not empty
        if not table_name:
            table_name = "data_table"
        
        self.logger.debug(f"Table name derived: {table_name} from {csv_file.name}")
        return table_name
    
    def detect_delimiter(self, csv_file: Path, sample_size: int = 1024) -> str:
        """Detect the delimiter used in the CSV file."""
        try:
            with open(csv_file, 'r', encoding='utf-8') as f:
                sample = f.read(sample_size)
                sniffer = csv.Sniffer()
                delimiter = sniffer.sniff(sample).delimiter
                self.logger.debug(f"Detected delimiter: '{delimiter}' for {csv_file.name}")
                return delimiter
        except Exception as e:
            self.logger.warning(f"Could not detect delimiter for {csv_file.name}, using comma: {e}")
            return ','
    
    def read_csv_data(self, csv_file: Path) -> Tuple[List[str], List[Dict[str, str]]]:
        """Read CSV file and return headers and data rows."""
        self.logger.debug(f"Reading CSV file: {csv_file}")
        
        delimiter = self.detect_delimiter(csv_file)
        headers = []
        data_rows = []
        
        try:
            with open(csv_file, 'r', encoding='utf-8', newline='') as f:
                # First, read the headers
                reader = csv.reader(f, delimiter=delimiter)
                headers = next(reader)
                
                # Clean up headers - remove whitespace and handle empty headers
                cleaned_headers = []
                for i, header in enumerate(headers):
                    cleaned_header = header.strip()
                    if not cleaned_header:
                        cleaned_header = f"column_{i+1}"
                    # Replace special characters with underscores for SQL compatibility
                    cleaned_header = re.sub(r'[^a-zA-Z0-9_]', '_', cleaned_header)
                    cleaned_headers.append(cleaned_header)
                
                headers = cleaned_headers
                self.logger.debug(f"Column headers found: {headers}")
                
                # Read the data rows
                f.seek(0)  # Reset file pointer
                dict_reader = csv.DictReader(f, delimiter=delimiter)
                
                for row_num, row in enumerate(dict_reader, 1):
                    # Create a clean row with our cleaned headers
                    clean_row = {}
                    original_headers = list(row.keys())
                    
                    for i, clean_header in enumerate(headers):
                        if i < len(original_headers):
                            original_header = original_headers[i]
                            clean_row[clean_header] = row.get(original_header, '').strip()
                        else:
                            clean_row[clean_header] = ''
                    
                    data_rows.append(clean_row)
                
                self.logger.debug(f"Read {len(data_rows)} data rows from {csv_file.name}")
                
        except Exception as e:
            self.logger.error(f"Error reading CSV file {csv_file}: {e}")
            return [], []
        
        return headers, data_rows
    
    def escape_sql_string(self, value: str) -> str:
        """Escape single quotes in SQL string values and handle None/empty values."""
        if value is None:
            return 'NULL'
        
        # Convert to string and escape single quotes
        str_value = str(value).replace("'", "''")
        return f"'{str_value}'"
    
    def generate_tsql_inserts(self, table_name: str, headers: List[str], data_rows: List[Dict[str, str]]) -> List[str]:
        """Generate T-SQL INSERT statements from CSV data."""
        if not headers or not data_rows:
            return []
        
        statements = []
        
        # Add header comments
        statements.append(f"-- T-SQL INSERT statements for table: {table_name}")
        statements.append(f"-- Generated from CSV file")
        statements.append(f"-- Columns: {', '.join(headers)}")
        statements.append(f"-- Total rows: {len(data_rows)}")
        statements.append("")
        
        # Generate INSERT statements
        columns_str = ', '.join(headers)
        
        for row_num, row in enumerate(data_rows, 1):
            values = []
            for header in headers:
                value = row.get(header, '')
                escaped_value = self.escape_sql_string(value)
                values.append(escaped_value)
            
            values_str = ', '.join(values)
            insert_statement = f"INSERT INTO {table_name} ({columns_str}) VALUES ({values_str});"
            statements.append(insert_statement)
            
            # Log progress for large files
            if row_num % 1000 == 0:
                self.logger.debug(f"Generated {row_num} INSERT statements")
        
        return statements
    
    def save_sql_file(self, table_name: str, csv_file: Path, sql_statements: List[str]):
        """Save T-SQL statements to a .sql file."""
        output_filename = f"{csv_file.stem}.sql"
        output_path = self.output_dir / output_filename
        
        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write('\n'.join(sql_statements))
            
            self.logger.info(f"SQL file saved: {output_path}")
            self.logger.debug(f"Generated {len(sql_statements)} total lines (including comments)")
            
        except IOError as e:
            self.logger.error(f"Error saving SQL file {output_path}: {e}")
    
    def process_csv_file(self, csv_file: Path):
        """Process a single CSV file and generate T-SQL output."""
        self.logger.info(f"Processing CSV file: {csv_file}")
        
        # Derive table name
        table_name = self.derive_table_name(csv_file)
        self.logger.debug(f"Table name derived: {table_name}")
        
        # Read CSV data and locate headers
        self.logger.debug(f"Reading file and locating column headers: {csv_file}")
        headers, data_rows = self.read_csv_data(csv_file)
        
        if not headers:
            self.logger.warning(f"No headers found in CSV file: {csv_file}")
            return
        
        if not data_rows:
            self.logger.warning(f"No data rows found in CSV file: {csv_file}")
            return
        
        self.logger.info(f"Found {len(headers)} columns and {len(data_rows)} data rows")
        
        # Generate T-SQL INSERT statements
        self.logger.debug(f"Generating T-SQL INSERT statements for table: {table_name}")
        sql_statements = self.generate_tsql_inserts(table_name, headers, data_rows)
        
        if not sql_statements:
            self.logger.warning(f"No SQL statements generated for: {csv_file}")
            return
        
        # Save to output file
        self.logger.debug(f"Saving {len(data_rows)} INSERT statements to output file")
        self.save_sql_file(table_name, csv_file, sql_statements)
        
        self.logger.info(f"Successfully processed: {csv_file.name} -> {table_name}.sql")
    
    def convert_directory(self, directory: str):
        """Convert all CSV files in the given directory."""
        self.logger.info(f"Starting CSV to T-SQL conversion for directory: {directory}")
        
        csv_files = self.find_csv_files(directory)
        
        if not csv_files:
            self.logger.warning("No CSV files found in the specified directory")
            return
        
        processed_count = 0
        for csv_file in csv_files:
            try:
                self.process_csv_file(csv_file)
                processed_count += 1
            except Exception as e:
                self.logger.error(f"Error processing {csv_file}: {e}")
                continue
        
        self.logger.info(f"Conversion completed. Successfully processed {processed_count} out of {len(csv_files)} CSV files")
        self.logger.info(f"Output files saved to: {self.output_dir}")


def main():
    """Main function to handle command line arguments and run the converter."""
    parser = argparse.ArgumentParser(
        description='Convert CSV files to T-SQL INSERT statements based on actual column headers',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  python csv_to_tsql.py ./csv_files
  python csv_to_tsql.py /path/to/csv --debug
  python csv_to_tsql.py . --debug
  
The script will:
1. Find all .csv files recursively
2. Read the column headers from each file
3. Generate table names from filenames (e.g., sales_data.csv -> sales_data)
4. Create INSERT statements with proper column mapping
5. Save .sql files to output_sql/ directory
        '''
    )
    
    parser.add_argument(
        'directory',
        nargs='?',
        default='.',
        help='Directory to scan for CSV files (default: current directory)'
    )
    
    parser.add_argument(
        '--debug',
        action='store_true',
        help='Enable debug mode with verbose logging'
    )
    
    args = parser.parse_args()
    
    # Create converter instance
    converter = CSVToTSQLConverter(debug=args.debug)
    
    # Process the directory
    converter.convert_directory(args.directory)


if __name__ == "__main__":
    main()