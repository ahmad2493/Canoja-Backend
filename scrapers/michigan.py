import pandas as pd
import json
from datetime import datetime
import re
from typing import Dict, List, Optional, Any

class MichiganCannabisCSVProcessor:
    def __init__(self, csv_file_path: str):
        self.csv_file_path = csv_file_path
        self.df = None
        
    def load_csv_data(self):
        """Load CSV data into pandas DataFrame"""
        try:
            # Try reading with different encodings
            encodings = ['utf-8', 'latin-1', 'iso-8859-1', 'cp1252']
            
            for encoding in encodings:
                try:
                    self.df = pd.read_csv(self.csv_file_path, encoding=encoding)
                    print(f"Successfully loaded CSV with {encoding} encoding")
                    break
                except UnicodeDecodeError:
                    continue
            
            if self.df is None:
                raise Exception("Could not read CSV with any encoding")
            
            print(f"Loaded {len(self.df)} records from CSV file")
            print(f"Columns found: {list(self.df.columns)}")
            
            # Display first few rows for verification
            print("\nFirst 3 rows:")
            print(self.df.head(3).to_string())
            
            return True
        except Exception as e:
            print(f"Error loading CSV file: {e}")
            return False
    
    def clean_string(self, value) -> Optional[str]:
        """Clean and normalize string values"""
        if pd.isna(value) or value == '' or str(value).lower() in ['none', 'null', 'n/a', 'nan']:
            return None
        return str(value).strip()
    
    def parse_date(self, date_value):
        """Parse date values and return datetime object for MongoDB"""
        if pd.isna(date_value) or not date_value:
            return None
            
        try:
            if isinstance(date_value, str):
                # Handle MM/DD/YYYY format common in US data
                parsed_date = pd.to_datetime(date_value, errors='coerce')
            else:
                # Handle Excel datetime objects
                parsed_date = pd.to_datetime(date_value)
            
            if pd.isna(parsed_date):
                return None
                
            # Return datetime object instead of ISO string for MongoDB
            return parsed_date.to_pydatetime()
        except Exception as e:
            print(f"Date parsing error for value '{date_value}': {e}")
            return None
    
    def determine_license_type(self, record_type: str) -> str:
        """Determine license type based on Michigan record type"""
        if not record_type:
            return 'other'
        
        record_lower = record_type.lower()
        
        if 'retailer' in record_lower or 'retail' in record_lower:
            return 'retail'
        elif 'processor' in record_lower or 'processing' in record_lower:
            return 'processing'
        elif 'grower' in record_lower or 'cultivation' in record_lower:
            return 'cultivation'
        elif 'transporter' in record_lower or 'transport' in record_lower:
            return 'distribution'
        elif 'testing' in record_lower or 'lab' in record_lower:
            return 'testing'
        elif 'entity' in record_lower:
            return 'entity'
        elif 'secure transport' in record_lower:
            return 'transport'
        else:
            return 'other'
    
    def parse_license_status(self, status: str) -> str:
        """Parse and standardize license status"""
        if not status:
            return 'Unknown'
        
        status_lower = status.lower()
        if 'active' in status_lower:
            if 'prequalified' in status_lower:
                return 'Prequalified'
            else:
                return 'Active'
        elif 'inactive' in status_lower:
            return 'Inactive'
        elif 'expired' in status_lower:
            return 'Expired'
        elif 'pending' in status_lower:
            return 'Pending'
        elif 'suspended' in status_lower:
            return 'Suspended'
        else:
            return 'Unknown'
    
    def parse_address(self, address_str: str) -> Dict[str, Optional[str]]:
        """Parse address string to extract city"""
        if not address_str:
            return {'full_address': None, 'city': None}
        
        # Michigan addresses typically: "Street, City MI ZIP"
        parts = address_str.split(',')
        city = None
        
        if len(parts) >= 2:
            # Last part usually contains city, state, zip
            city_state_zip = parts[-1].strip()
            # Extract city (everything before MI and ZIP)
            city_match = re.search(r'^(.+?)\s+MI\s+\d{5}', city_state_zip)
            if city_match:
                city = city_match.group(1).strip()
            else:
                # Try alternative format
                city_parts = city_state_zip.split()
                if len(city_parts) >= 2:
                    city = ' '.join(city_parts[:-2]) if len(city_parts) > 2 else city_parts[0]
        
        return {
            'full_address': address_str,
            'city': city
        }
    
    def is_smoke_shop(self, business_name: str) -> bool:
        """Determine if establishment is a smoke shop"""
        if not business_name:
            return False
            
        name_lower = business_name.lower()
        return any(word in name_lower for word in ['smoke', 'tobacco', 'vape', 'cigar', 'head shop'])
    
    def transform_row_to_schema(self, row) -> Dict[str, Any]:
        """Transform a single CSV row to match the MongoDB schema"""
        
        # Map CSV columns to our fields (adjust these based on your actual column names)
        record_number = self.clean_string(row.get('Record Number') or row.get('RecordNumber'))
        record_type = self.clean_string(row.get('Record Type') or row.get('RecordType'))
        license_name = self.clean_string(row.get('License Name') or row.get('LicenseName'))
        address = self.clean_string(row.get('Address'))
        expiration_date = self.clean_string(row.get('Expiration Date') or row.get('ExpirationDate'))
        status = self.clean_string(row.get('Status'))
        notes = self.clean_string(row.get('Notes'))
        disciplinary_action = self.clean_string(row.get('Disciplinary Action') or row.get('DisciplinaryAction'))
        
        # Parse address
        address_info = self.parse_address(address)
        
        # Determine license properties
        license_type = self.determine_license_type(record_type)
        license_status = self.parse_license_status(status)
        smoke_shop = self.is_smoke_shop(license_name)
        
        # Parse dates
        expiry_date = self.parse_date(expiration_date)
        
        # Create the document according to your Mongoose schema
        document = {
            'googlePlaceId': None,
            'business_name': license_name,
            'license_number': record_number,
            'stateName': 'Michigan',
            'city': address_info['city'],
            'business_address': address_info['full_address'],
            'contact_information': {
                'phone': None,  # Not available in CSV
                'email': None,  # Not available in CSV
                'website': None  # Not available in CSV
            },
            'owner': {
                'name': None,  # Not available in CSV
                'email': None,
                'role': None,
                'phone': None,
                'govt_issued_id': None
            },
            'operator_name': None,  # Not available in CSV
            'issue_date': None,  # Not available in CSV
            'expiration_date': expiry_date,  # datetime object
            'license_type': license_type,
            'license_status': license_status,
            'jurisdiction': 'Michigan',
            'regulatory_body': 'Cannabis Regulatory Agency (CRA)',
            'entity_type': [license_type],
            'filing_documents_url': None,
            'license_conditions': [],
            'claimed': False,
            'claimedBy': None,
            'claimedAt': None,
            'canojaVerified': False,  # Changed to False to match schema default
            'adminVerificationRequired': False,
            'featured': False,
            'dba': None,  # Not available in CSV
            'state_license_document': None,
            'utility_bill': None,
            'gps_validation': False,
            'location': {
                'type': 'Point',
                'coordinates': []  # Empty array when no coordinates available
            },
            'smoke_shop': smoke_shop,
            # Additional Michigan-specific fields (not in schema but useful for reference)
            '_michigan_record_number': record_number,
            '_michigan_record_type': record_type,
            '_michigan_notes': notes,
            '_michigan_disciplinary_action': disciplinary_action,
            '_michigan_raw_status': status
        }
        
        return document
    
    def process_all_data(self) -> List[Dict[str, Any]]:
        """Process all data from CSV and return list of documents"""
        if not self.load_csv_data():
            return []
        
        documents = []
        processed_count = 0
        error_count = 0
        
        for index, row in self.df.iterrows():
            try:
                document = self.transform_row_to_schema(row)
                
                # Only include records with essential data
                if document['business_name'] and document['license_number']:
                    documents.append(document)
                    processed_count += 1
                else:
                    print(f"Skipping row {index + 1}: Missing essential data (business_name or license_number)")
                    
            except Exception as e:
                print(f"Error processing row {index + 1}: {e}")
                error_count += 1
                continue
        
        print(f"Successfully processed {processed_count} records")
        print(f"Errors encountered: {error_count}")
        return documents
    
    def save_to_json(self, documents: List[Dict[str, Any]], output_file: str = 'michigan_processed.json'):
        """Save processed data to JSON file"""
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(documents, f, indent=2, ensure_ascii=False, default=str)
            print(f"Data saved to {output_file}")
        except Exception as e:
            print(f"Error saving to JSON: {e}")
    
    def print_sample_data(self, documents: List[Dict[str, Any]], num_samples: int = 3):
        """Print sample transformed data for verification"""
        print(f"\n=== Sample of {min(num_samples, len(documents))} transformed records ===")
        for i, doc in enumerate(documents[:num_samples]):
            print(f"\nRecord {i + 1}:")
            print(f"Business Name: {doc['business_name']}")
            print(f"License Number: {doc['license_number']}")
            print(f"Record Type: {doc.get('_michigan_record_type', 'N/A')}")
            print(f"License Type: {doc['license_type']}")
            print(f"City: {doc['city']}, {doc['stateName']}")
            print(f"Address: {doc['business_address']}")
            print(f"License Status: {doc['license_status']}")
            print(f"Expiration Date: {doc['expiration_date']}")
            print(f"Notes: {doc.get('_michigan_notes', 'N/A')}")
            print(f"Smoke Shop: {doc['smoke_shop']}")
            print("-" * 60)
    
    def print_statistics(self, documents: List[Dict[str, Any]]):
        """Print statistics about the processed data"""
        if not documents:
            print("No documents to analyze")
            return
            
        print(f"\n=== Michigan Cannabis License Statistics ===")
        print(f"Total records: {len(documents)}")
        
        # Various statistics
        license_types = {}
        cities = {}
        statuses = {}
        record_types = {}
        smoke_shops = 0
        with_expiry = 0
        with_notes = 0
        with_disciplinary = 0
        
        for doc in documents:
            # Count license types
            license_type = doc.get('license_type', 'unknown')
            license_types[license_type] = license_types.get(license_type, 0) + 1
            
            # Count cities
            city = doc.get('city', 'unknown')
            cities[city] = cities.get(city, 0) + 1
            
            # Count license statuses
            status = doc.get('license_status', 'unknown')
            statuses[status] = statuses.get(status, 0) + 1
            
            # Count record types (using Michigan-specific field)
            record_type = doc.get('_michigan_record_type', 'unknown')
            record_types[record_type] = record_types.get(record_type, 0) + 1
            
            # Count smoke shops
            if doc.get('smoke_shop'):
                smoke_shops += 1
                
            # Count records with expiry dates
            if doc.get('expiration_date'):
                with_expiry += 1
                
            # Count records with notes
            if doc.get('_michigan_notes'):
                with_notes += 1
                
            # Count records with disciplinary actions
            if doc.get('_michigan_disciplinary_action'):
                with_disciplinary += 1
        
        print(f"\nLicense Types:")
        for license_type, count in sorted(license_types.items()):
            print(f"  {license_type}: {count}")
            
        print(f"\nRecord Types:")
        for record_type, count in sorted(record_types.items()):
            print(f"  {record_type}: {count}")
            
        print(f"\nLicense Statuses:")
        for status, count in sorted(statuses.items()):
            print(f"  {status}: {count}")
            
        print(f"\nTop 15 Cities:")
        for city, count in sorted(cities.items(), key=lambda x: x[1], reverse=True)[:15]:
            print(f"  {city or 'Unknown'}: {count}")
            
        print(f"\nOther Statistics:")
        print(f"  Records with expiry dates: {with_expiry}")
        print(f"  Records with notes: {with_notes}")
        print(f"  Records with disciplinary actions: {with_disciplinary}")
        print(f"  Smoke shops: {smoke_shops}")


# MongoDB integration
def insert_to_mongodb(documents: List[Dict[str, Any]], connection_string: str = 'mongodb://localhost:27017/', 
                     database_name: str = 'cannabis_licenses', collection_name: str = 'testrecords'):
    """
    Insert processed data into MongoDB
    Requires: pip install pymongo
    """
    try:
        from pymongo import MongoClient
        
        client = MongoClient(connection_string)
        db = client[database_name]
        collection = db[collection_name]
        
        if documents:
            # Clear existing Michigan data (optional)
            # collection.delete_many({'stateName': 'Michigan'})
            
            # Insert documents
            result = collection.insert_many(documents)
            print(f"Inserted {len(result.inserted_ids)} documents into MongoDB")
            
            # Create indexes for better performance
            collection.create_index([("location", "2dsphere")])
            collection.create_index("business_name")
            collection.create_index("license_number")
            collection.create_index("record_number")
            collection.create_index("license_status")
            collection.create_index([("city", 1), ("stateName", 1)])
            collection.create_index("license_type")
            collection.create_index("record_type")
            collection.create_index("smoke_shop")
            
            print("Created database indexes")
        else:
            print("No documents to insert")
        
        client.close()
    except ImportError:
        print("PyMongo not installed. Run: pip install pymongo")
    except Exception as e:
        print(f"Error inserting to MongoDB: {e}")


if __name__ == "__main__":
    # Initialize processor with your CSV file path
    csv_file_path = 'RecordList20250917.csv'  # Update this path to match your CSV file name
    processor = MichiganCannabisCSVProcessor(csv_file_path)
    
    # Process all data
    print("Starting data processing from CSV...")
    processed_data = processor.process_all_data()
    
    if processed_data:
        # Print sample data for verification
        processor.print_sample_data(processed_data, 5)
        
        # Print statistics
        processor.print_statistics(processed_data)
        
        # Save to processed JSON file (optional)
        processor.save_to_json(processed_data, 'michigan_processed.json')
        
        # Insert to MongoDB
        print("\nInserting data to MongoDB...")
        insert_to_mongodb(processed_data)
        
        print(f"\n✅ Processing complete! Total records processed: {len(processed_data)}")
        
    else:
        print("❌ No data could be processed from the CSV file")
        print("Please check the file path and CSV format")

"""
Michigan Cannabis License CSV to MongoDB Processor

This script processes Michigan cannabis license data from CSV format and stores it in MongoDB.

The script handles the CSV structure visible in your screenshot:
- Record Number -> license_number
- Record Type -> license_type (mapped to standard types)
- License Name -> business_name  
- Address -> business_address (parsed for city)
- Expiration Date -> expiration_date
- Status -> license_status (standardized)
- Notes -> notes (Michigan-specific field)
- Disciplinary Action -> disciplinary_action (Michigan-specific field)

Required dependencies:
pip install pymongo pandas

The script will:
1. Read your CSV file with proper encoding handling
2. Transform data to match your MongoDB schema
3. Store in MongoDB collection 'michigan_records'
4. Create appropriate database indexes
5. Print statistics and sample data

Key features:
- Handles various CSV encodings
- Maps Michigan record types to standard license types
- Parses addresses to extract city names
- Standardizes status values
- Preserves Michigan-specific fields
"""