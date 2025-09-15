import pandas as pd
import json
from datetime import datetime
import re
from typing import Dict, List, Optional, Any

class BCCannabisLicenseScraper:
    def __init__(self, file_path: str):
        self.file_path = file_path
        self.df = None
        
    def load_data(self):
        """Load data from CSV/Excel file into pandas DataFrame"""
        try:
            # Try different file formats
            if self.file_path.endswith('.csv'):
                self.df = pd.read_csv(self.file_path)
            elif self.file_path.endswith(('.xls', '.xlsx')):
                self.df = pd.read_excel(self.file_path)
            else:
                # Try CSV first, then Excel
                try:
                    self.df = pd.read_csv(self.file_path)
                except:
                    self.df = pd.read_excel(self.file_path)
            
            print(f"Loaded {len(self.df)} records from file")
            print(f"Columns found: {list(self.df.columns)}")
            return True
        except Exception as e:
            print(f"Error loading file: {e}")
            return False
    
    def clean_string(self, value) -> Optional[str]:
        """Clean and normalize string values"""
        if pd.isna(value) or value == '' or str(value).strip() == '':
            return None
        return str(value).strip()
    
    def parse_phone_number(self, phone: str) -> Optional[str]:
        """Clean and format phone numbers"""
        if not phone or pd.isna(phone):
            return None
        
        # Remove all non-digit characters
        digits_only = re.sub(r'\D', '', str(phone))
        
        # Format as standard phone number if we have 10 digits
        if len(digits_only) == 10:
            return f"({digits_only[:3]}) {digits_only[3:6]}-{digits_only[6:]}"
        elif len(digits_only) == 11 and digits_only[0] == '1':
            return f"1-({digits_only[1:4]}) {digits_only[4:7]}-{digits_only[7:]}"
        
        return digits_only if digits_only else None
    
    def determine_license_type(self, establishment_name: str) -> str:
        """Determine license type based on establishment name"""
        if not establishment_name:
            return 'retail'
            
        name = establishment_name.lower()
        
        # All entries in BC data appear to be retail cannabis stores
        if any(word in name for word in ['cultivation', 'grow', 'farm', 'grower']):
            return 'cultivation'
        elif any(word in name for word in ['processing', 'extraction', 'manufacturing', 'processor']):
            return 'processing'
        elif any(word in name for word in ['distribution', 'transport', 'delivery', 'distributor']):
            return 'distribution'
        elif any(word in name for word in ['testing', 'lab', 'laboratory']):
            return 'testing'
        else:
            # Default to retail for BC cannabis stores
            return 'retail'
    
    def is_smoke_shop(self, establishment_name: str) -> bool:
        """Determine if establishment is a smoke shop"""
        if not establishment_name:
            return False
            
        name = establishment_name.lower()
        # These are cannabis retail stores, not traditional smoke shops
        return any(word in name for word in ['smoke', 'tobacco', 'cigar']) and 'cannabis' not in name
    
    def create_full_address(self, address: str, city: str, postal: str) -> str:
        """Create full address string from components"""
        address_parts = []
        
        if address:
            address_parts.append(str(address).strip())
        
        location_parts = []
        if city:
            location_parts.append(str(city).strip())
        
        # Add province
        location_parts.append('BC')
        
        if postal:
            location_parts.append(str(postal).strip())
        
        if location_parts:
            address_parts.append(', '.join(location_parts))
        
        return ', '.join(address_parts) if address_parts else ''
    
    def extract_postal_code(self, postal_value: str) -> Optional[str]:
        """Extract and format postal code"""
        if not postal_value or pd.isna(postal_value):
            return None
        
        postal = str(postal_value).strip().upper()
        
        # Canadian postal code pattern: A1A 1A1
        if len(postal) == 6:
            # Add space if missing: A1A1A1 -> A1A 1A1
            return f"{postal[:3]} {postal[3:]}"
        elif len(postal) == 7 and postal[3] == ' ':
            return postal
        
        return postal if postal else None
    
    def transform_row_to_schema(self, row) -> Dict[str, Any]:
        """Transform a single row to match the MongoDB schema"""
        
        # Extract basic information from the columns shown in screenshot
        license_number = self.clean_string(row.get('Licence'))  # Column A
        establishment_name = self.clean_string(row.get('Establishment Name'))  # Column B
        phone = self.parse_phone_number(row.get('Phone'))  # Column C
        address = self.clean_string(row.get('Address'))  # Column D
        city = self.clean_string(row.get('City'))  # Column E
        postal = self.extract_postal_code(row.get('Postal'))  # Column F
        status = self.clean_string(row.get('Status'))  # Column G
        
        # Create full address
        business_address = self.create_full_address(address, city, postal)
        
        # Determine license type and smoke shop status
        license_type = self.determine_license_type(establishment_name)
        smoke_shop = self.is_smoke_shop(establishment_name)
        
        # Map status from Excel to our schema
        license_status = 'Active' if status and status.lower() == 'open' else 'Inactive'
        
        # Create the document according to your MongoDB schema
        document = {
            'business_name': establishment_name,
            'license_number': license_number,
            'stateName': 'British Columbia',
            'city': city,
            'business_address': business_address,
            'contact_information': {
                'phone': phone,
                'email': None,  # Not available in the data
                'website': None  # Not available in the data
            },
            'owner': {
                'name': None,  # Not available in the data
                'email': None,
                'role': None,
                'phone': phone,
                'govt_issued_id': None
            },
            'operator_name': None,  # Not available in the data
            'issue_date': None,  # Not available in the data
            'expiration_date': None,  # Not available in the data
            'license_type': license_type,
            'license_status': license_status,
            'jurisdiction': 'British Columbia',
            'regulatory_body': 'Liquor and Cannabis Regulation Branch',
            'entity_type': [license_type],
            'filing_documents_url': None,
            'license_conditions': [],
            'claimed': False,
            'claimedBy': None,
            'claimedAt': None,
            'canojaVerified': True,
            'adminVerificationRequired': False,
            'featured': False,
            'dba': establishment_name,
            'state_license_document': None,
            'utility_bill': None,
            'gps_validation': False,
            'location': {
                'type': 'Point',
                'coordinates': []  # Would need geocoding service to get lat/lng
            },
            'smoke_shop': smoke_shop
        }
        
        return document
    
    def scrape_all_data(self) -> List[Dict[str, Any]]:
        """Scrape all data and return list of documents"""
        if not self.load_data():
            return []
        
        documents = []
        
        for index, row in self.df.iterrows():
            try:
                document = self.transform_row_to_schema(row)
                documents.append(document)
            except Exception as e:
                print(f"Error processing row {index}: {e}")
                print(f"Row data: {row.to_dict()}")
                continue
        
        print(f"Successfully processed {len(documents)} records")
        return documents
    
    def save_to_json(self, documents: List[Dict[str, Any]], output_file: str = 'british_columbia.json'):
        """Save scraped data to JSON file"""
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(documents, f, indent=2, ensure_ascii=False, default=str)
            print(f"Data saved to {output_file}")
        except Exception as e:
            print(f"Error saving to JSON: {e}")
    
    def print_sample_data(self, documents: List[Dict[str, Any]], num_samples: int = 5):
        """Print sample transformed data for verification"""
        print(f"\n=== Sample of {min(num_samples, len(documents))} transformed records ===")
        for i, doc in enumerate(documents[:num_samples]):
            print(f"\nRecord {i + 1}:")
            print(f"Business Name: {doc['business_name']}")
            print(f"License Number: {doc['license_number']}")
            print(f"City: {doc['city']}, {doc['stateName']}")
            print(f"Address: {doc['business_address']}")
            print(f"License Type: {doc['license_type']}")
            print(f"License Status: {doc['license_status']}")
            print(f"Phone: {doc['contact_information']['phone']}")
            print(f"Smoke Shop: {doc['smoke_shop']}")
            print("-" * 60)
    
    def get_summary_stats(self, documents: List[Dict[str, Any]]):
        """Print summary statistics"""
        print(f"\n=== Summary Statistics ===")
        print(f"Total Records: {len(documents)}")
        
        # Count by city
        cities = {}
        license_types = {}
        statuses = {}
        
        for doc in documents:
            city = doc.get('city', 'Unknown')
            cities[city] = cities.get(city, 0) + 1
            
            license_type = doc.get('license_type', 'Unknown')
            license_types[license_type] = license_types.get(license_type, 0) + 1
            
            status = doc.get('license_status', 'Unknown')
            statuses[status] = statuses.get(status, 0) + 1
        
        print(f"\nTop 10 Cities:")
        for city, count in sorted(cities.items(), key=lambda x: x[1], reverse=True)[:10]:
            print(f"  {city}: {count}")
        
        print(f"\nLicense Types:")
        for license_type, count in license_types.items():
            print(f"  {license_type}: {count}")
        
        print(f"\nLicense Status:")
        for status, count in statuses.items():
            print(f"  {status}: {count}")


# Usage example
if __name__ == "__main__":
    # Initialize scraper with your file path
    # Update this path to match your actual file location
    scraper = BCCannabisLicenseScraper('BC-Retail-Cannabis-Stores.csv')  # or .xlsx
    
    # Scrape all data
    scraped_data = scraper.scrape_all_data()
    
    if scraped_data:
        # Print sample data for verification
        scraper.print_sample_data(scraped_data, 5)
        
        # Print summary statistics
        scraper.get_summary_stats(scraped_data)
        
        # Save to JSON file
        scraper.save_to_json(scraped_data)
        
        print(f"\nTotal records processed: {len(scraped_data)}")
        
        # Print column mapping info
        print("\n=== Column Mapping ===")
        print("Excel Column -> Schema Field")
        print("Licence (A) -> license_number")
        print("Establishment Name (B) -> business_name")
        print("Phone (C) -> contact_information.phone")
        print("Address (D) -> business_address (part)")
        print("City (E) -> city")
        print("Postal (F) -> business_address (part)")
        print("Status (G) -> license_status")
    else:
        print("No data could be processed from the file")


# Additional utility functions for MongoDB operations
def insert_to_mongodb(documents: List[Dict[str, Any]], connection_string: str, database_name: str, collection_name: str = 'licenserecords'):
    """
    Insert scraped data into MongoDB
    Requires: pip install pymongo
    """
    try:
        from pymongo import MongoClient
        
        client = MongoClient(connection_string)
        db = client[database_name]
        collection = db[collection_name]
        
        # Insert documents
        result = collection.insert_many(documents)
        print(f"Inserted {len(result.inserted_ids)} documents into MongoDB")
        
        client.close()
    except ImportError:
        print("PyMongo not installed. Run: pip install pymongo")
    except Exception as e:
        print(f"Error inserting to MongoDB: {e}")


# Example of using the MongoDB insertion
def run_full_pipeline():
    """Run the complete scraping and insertion pipeline"""
    # Initialize scraper
    scraper = BCCannabisLicenseScraper('BC-Retail-Cannabis-Stores.csv')
    
    # Scrape data
    scraped_data = scraper.scrape_all_data()
    
    if scraped_data:
        # Save to JSON
        scraper.save_to_json(scraped_data)

        # Insert to MongoDB (uncomment and configure as needed)
        insert_to_mongodb(
            scraped_data,
            'mongodb://localhost:27017/',  # Your MongoDB connection string
            'cannabis_licenses',           # Database name
            'testrecords'              # Collection name
        )
        
        return scraped_data
    
    return []


# Uncomment to run the full pipeline
if __name__ == "__main__":
    run_full_pipeline()