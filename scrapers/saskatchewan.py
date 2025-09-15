import pandas as pd
import json
import os
from datetime import datetime
import re
from typing import Dict, List, Optional, Any

class SLGACannabisRetailerScraper:
    def __init__(self, excel_file_path: str):
        self.excel_file_path = excel_file_path
        self.df = None
        
        # Debug: Check if file exists
        if os.path.exists(self.excel_file_path):
            print(f"✓ File found: {self.excel_file_path}")
        else:
            print(f"✗ File not found: {self.excel_file_path}")
            print(f"Current working directory: {os.getcwd()}")
            print("Files in current directory:")
            for file in os.listdir('.'):
                if file.endswith(('.xls', '.xlsx', '.csv')):
                    print(f"  - {file}")
    
    def load_excel_data(self):
        """Load Excel data into pandas DataFrame"""
        # First check if file exists
        if not os.path.exists(self.excel_file_path):
            print(f"Error: File '{self.excel_file_path}' not found!")
            print(f"Current working directory: {os.getcwd()}")
            print("Available Excel files in current directory:")
            excel_files = [f for f in os.listdir('.') if f.endswith(('.xls', '.xlsx', '.csv'))]
            if excel_files:
                for file in excel_files:
                    print(f"  - {file}")
                print(f"\nTry using one of these files instead.")
            else:
                print("  No Excel files found in current directory")
            return False
            
        try:
            # Determine file extension and use appropriate engine
            if self.excel_file_path.lower().endswith('.xls'):
                # Use xlrd for older .xls files
                self.df = pd.read_excel(self.excel_file_path, engine='xlrd')
            elif self.excel_file_path.lower().endswith('.xlsx'):
                # Use openpyxl for newer .xlsx files
                self.df = pd.read_excel(self.excel_file_path, engine='openpyxl')
            else:
                # Try auto-detection
                self.df = pd.read_excel(self.excel_file_path)
            
            print(f"✓ Loaded {len(self.df)} records from Excel file")
            print(f"✓ Columns found: {list(self.df.columns)}")
            return True
        except Exception as e:
            print(f"Error loading Excel file: {e}")
            print(f"Make sure you have the required dependencies installed:")
            print("For .xls files: pip install xlrd")
            print("For .xlsx files: pip install openpyxl")
            return False
    
    def clean_string(self, value) -> Optional[str]:
        """Clean and normalize string values"""
        if pd.isna(value) or value == '':
            return None
        return str(value).strip()
    
    def parse_date(self, date_value) -> Optional[str]:
        """Parse date values and return ISO format string"""
        if pd.isna(date_value):
            return None
            
        try:
            if isinstance(date_value, str):
                # Parse string dates like "2025-09-09"
                parsed_date = pd.to_datetime(date_value)
            else:
                # Handle Excel datetime objects
                parsed_date = pd.to_datetime(date_value)
            
            return parsed_date.isoformat()
        except:
            return None
    
    def extract_phone_from_website_or_name(self, website: str, business_name: str) -> Optional[str]:
        """Try to extract phone number from website or business name (if available)"""
        # This is a placeholder - in practice, you might need to scrape websites
        # or have additional data sources for phone numbers
        return None
    
    def determine_license_type_from_name(self, operating_name: str) -> str:
        """Determine license type based on operating name"""
        if not operating_name:
            return 'retail'
        
        name = operating_name.lower()
        
        # SLGA retailers are primarily retail outlets
        if any(word in name for word in ['dispensary', 'cannabis', 'weed', 'shop', 'store']):
            return 'retail'
        elif 'smoke' in name:
            return 'retail'  # Still retail but might be smoke shop
        else:
            return 'retail'  # Default to retail for SLGA data
    
    def is_smoke_shop(self, operating_name: str) -> bool:
        """Determine if establishment is a smoke shop"""
        if not operating_name:
            return False
            
        name = operating_name.lower()
        return any(word in name for word in ['smoke', 'tobacco', 'vape', 'cigar'])
    
    def create_full_address(self, street_address: str, city: str) -> str:
        """Create full address string"""
        address_parts = []
        
        if street_address and street_address.strip():
            address_parts.append(street_address.strip())
        
        if city and city.strip():
            address_parts.append(city.strip())
        
        # Add Saskatchewan, Canada
        if address_parts:
            address_parts.extend(['Saskatchewan', 'Canada'])
        
        return ', '.join(address_parts)
    
    def clean_website_url(self, website: str) -> Optional[str]:
        """Clean and validate website URL"""
        if not website or pd.isna(website) or website.lower() in ['n/a', 'none', '']:
            return None
        
        website = website.strip()
        
        # Add https:// if not present
        if website and not website.startswith(('http://', 'https://')):
            website = 'https://' + website
        
        return website
    
    def transform_row_to_schema(self, row) -> Dict[str, Any]:
        """Transform a single Excel row to match the MongoDB schema"""
        
        # Extract basic information
        permit_number = self.clean_string(row.get('Permit Number'))
        status = self.clean_string(row.get('Status'))
        city = self.clean_string(row.get('City'))
        operating_name = self.clean_string(row.get('Operating Name'))
        street_address = self.clean_string(row.get('Street Address'))
        website = self.clean_website_url(row.get('Website'))
        last_updated = self.parse_date(row.get('Last Updated'))
        
        # Create business address
        business_address = self.create_full_address(street_address, city)
        
        # Determine license type and smoke shop status
        license_type = self.determine_license_type_from_name(operating_name)
        smoke_shop = self.is_smoke_shop(operating_name)
        
        # Try to extract phone (placeholder - would need additional data source)
        phone = self.extract_phone_from_website_or_name(website, operating_name)
        
        # Create the document according to your schema
        document = {
            'business_name': operating_name,
            'license_number': permit_number,
            'stateName': 'Saskatchewan',  # All SLGA entries are in Saskatchewan
            'city': city,
            'business_address': business_address,
            'contact_information': {
                'phone': phone,
                'email': None,  # Not available in SLGA data
                'website': website
            },
            'owner': {
                'name': None,  # Not available in SLGA data
                'email': None,
                'role': None,
                'phone': phone,
                'govt_issued_id': None
            },
            'operator_name': operating_name,
            'issue_date': None,  # Not available in SLGA data
            'expiration_date': None,
            'license_type': license_type,
            'license_status': status,  # Active/Inactive from SLGA data
            'jurisdiction': 'Saskatchewan',
            'regulatory_body': 'Saskatchewan Liquor and Gaming Authority (SLGA)',
            'entity_type': [license_type],
            'filing_documents_url': None,
            'license_conditions': [],
            'claimed': False,
            'claimedBy': None,
            'claimedAt': None,
            'canojaVerified': True,  # Official SLGA data
            'adminVerificationRequired': False,
            'featured': False,
            'dba': operating_name,
            'state_license_document': None,
            'utility_bill': None,
            'gps_validation': False,
            'location': {
                'type': 'Point',
                'coordinates': []  # Would need geocoding for lat/lng
            },
            'smoke_shop': smoke_shop,
            # Additional SLGA-specific fields
            'permit_number': permit_number,
            'last_updated': last_updated,
            'data_source': 'SLGA Cannabis Retailers Registry'
        }
        
        return document
    
    def scrape_all_data(self) -> List[Dict[str, Any]]:
        """Scrape all data from Excel and return list of documents"""
        if not self.load_excel_data():
            return []
        
        documents = []
        
        for index, row in self.df.iterrows():
            try:
                document = self.transform_row_to_schema(row)
                documents.append(document)
            except Exception as e:
                print(f"Error processing row {index}: {e}")
                continue
        
        print(f"✓ Successfully processed {len(documents)} records")
        return documents
    
    def save_to_json(self, documents: List[Dict[str, Any]], output_file: str = 'saskatchewan.json'):
        """Save scraped data to JSON file"""
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(documents, f, indent=2, ensure_ascii=False, default=str)
            print(f"✓ Data saved to {output_file}")
        except Exception as e:
            print(f"Error saving to JSON: {e}")
    
    def print_sample_data(self, documents: List[Dict[str, Any]], num_samples: int = 3):
        """Print sample transformed data for verification"""
        print(f"\n=== Sample of {min(num_samples, len(documents))} transformed records ===")
        for i, doc in enumerate(documents[:num_samples]):
            print(f"\nRecord {i + 1}:")
            print(f"Business Name: {doc['business_name']}")
            print(f"Permit Number: {doc['license_number']}")
            print(f"Status: {doc['license_status']}")
            print(f"City: {doc['city']}")
            print(f"Address: {doc['business_address']}")
            print(f"Website: {doc['contact_information']['website']}")
            print(f"License Type: {doc['license_type']}")
            print(f"Smoke Shop: {doc['smoke_shop']}")
            print(f"Last Updated: {doc['last_updated']}")
            print("-" * 50)
    
    def get_statistics(self, documents: List[Dict[str, Any]]):
        """Print statistics about the scraped data"""
        if not documents:
            return
        
        print(f"\n=== SLGA Cannabis Retailers Statistics ===")
        print(f"Total Records: {len(documents)}")
        
        # Status breakdown
        status_counts = {}
        for doc in documents:
            status = doc['license_status']
            status_counts[status] = status_counts.get(status, 0) + 1
        
        print(f"\nStatus Breakdown:")
        for status, count in status_counts.items():
            print(f"  {status}: {count}")
        
        # City breakdown (top 10)
        city_counts = {}
        for doc in documents:
            city = doc['city']
            if city:
                city_counts[city] = city_counts.get(city, 0) + 1
        
        print(f"\nTop 10 Cities:")
        sorted_cities = sorted(city_counts.items(), key=lambda x: x[1], reverse=True)
        for city, count in sorted_cities[:10]:
            print(f"  {city}: {count}")
        
        # Smoke shops
        smoke_shops = sum(1 for doc in documents if doc['smoke_shop'])
        print(f"\nSmoke Shops: {smoke_shops}")
        
        # Websites available
        websites = sum(1 for doc in documents if doc['contact_information']['website'])
        print(f"Websites Available: {websites}")


# File finder utility function
def find_slga_files(directory='.'):
    """Find SLGA-related Excel files"""
    excel_extensions = ('.xls', '.xlsx', '.csv')
    slga_files = []
    
    for file in os.listdir(directory):
        if file.lower().endswith(excel_extensions):
            if any(keyword in file.lower() for keyword in ['slga', 'cannabis', 'retailer', 'saskatchewan']):
                slga_files.append(file)
    
    return slga_files


# MongoDB insertion function
def insert_to_mongodb(documents: List[Dict[str, Any]], connection_string: str, database_name: str):
    """Insert scraped data into MongoDB"""
    try:
        from pymongo import MongoClient
        
        client = MongoClient(connection_string)
        db = client[database_name]
        collection = db['testrecords']
        
        # Clear existing SLGA data (optional)
        # collection.delete_many({'regulatory_body': 'Saskatchewan Liquor and Gaming Authority (SLGA)'})
        
        # Insert documents
        result = collection.insert_many(documents)
        print(f"✓ Inserted {len(result.inserted_ids)} documents into MongoDB")
        
        client.close()
    except ImportError:
        print("PyMongo not installed. Run: pip install pymongo")
    except Exception as e:
        print(f"Error inserting to MongoDB: {e}")


# Usage example with file detection
if __name__ == "__main__":
    print("=== SLGA Cannabis Retailers Scraper ===")
    print(f"Current working directory: {os.getcwd()}")
    
    # Look for SLGA files
    slga_files = find_slga_files()
    available_files = [f for f in os.listdir('.') if f.endswith(('.xls', '.xlsx', '.csv'))]
    
    if slga_files:
        print(f"\nFound SLGA-related files:")
        for i, file in enumerate(slga_files, 1):
            print(f"  {i}. {file}")
        file_to_use = slga_files[0]
    elif available_files:
        print(f"\nNo SLGA-specific files found. Available Excel files:")
        for i, file in enumerate(available_files, 1):
            print(f"  {i}. {file}")
        file_to_use = available_files[0]
    else:
        print("\nNo Excel files found in current directory!")
        print("Please make sure your SLGA Excel file is in the same folder as this script.")
        exit(1)
    
    print(f"\nUsing file: {file_to_use}")
    
    # Initialize scraper
    scraper = SLGACannabisRetailerScraper(file_to_use)
    
    # Scrape all data
    scraped_data = scraper.scrape_all_data()
    
    if scraped_data and len(scraped_data) > 0:
        # Print statistics
        scraper.get_statistics(scraped_data)
        
        # Print sample data for verification
        scraper.print_sample_data(scraped_data, 3)
        
        # Save to JSON file
        scraper.save_to_json(scraped_data)
        
        print(f"\n✓ Total records processed: {len(scraped_data)}")
        
        # Insert to MongoDB
        try:
            insert_to_mongodb(
                scraped_data, 
                'mongodb://localhost:27017/', 
                'cannabis_licenses'
            )
        except Exception as e:
            print(f"MongoDB insertion failed: {e}")
        
        # Print column mapping info
        print("\n=== Column Mapping ===")
        print("SLGA Column -> Schema Field")
        print("Permit Number -> license_number")
        print("Operating Name -> business_name")
        print("Status -> license_status")
        print("City -> city")
        print("Street Address -> business_address")
        print("Website -> contact_information.website")
        print("Last Updated -> last_updated")
        
    else:
        print("No data could be processed from the Excel file")
        if scraped_data is not None:
            print("The file was loaded but no records were found or processed.")
    
    print("\n=== Processing completed ===")