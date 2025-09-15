import pandas as pd
import json
from datetime import datetime
import re
from typing import Dict, List, Optional, Any

class CannabisLicenseScraper:
    def __init__(self, excel_file_path: str):
        self.excel_file_path = excel_file_path
        self.df = None
        
    def load_excel_data(self):
        """Load Excel data into pandas DataFrame"""
        try:
            self.df = pd.read_excel(self.excel_file_path)
            print(f"Loaded {len(self.df)} records from Excel file")
            return True
        except Exception as e:
            print(f"Error loading Excel file: {e}")
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
                # Parse string dates like "7/10/2022"
                parsed_date = pd.to_datetime(date_value)
            else:
                # Handle Excel datetime objects
                parsed_date = pd.to_datetime(date_value)
            
            return parsed_date.isoformat()
        except:
            return None
    
    def extract_postal_code(self, address: str, site_postal: str = None) -> Optional[str]:
        """Extract postal code from address or site postal field"""
        if site_postal and str(site_postal).strip():
            return str(site_postal).strip()
        
        if address:
            # Canadian postal code pattern: A1A 1A1
            postal_match = re.search(r'[A-Z]\d[A-Z]\s?\d[A-Z]\d', str(address), re.IGNORECASE)
            if postal_match:
                return postal_match.group(0).upper()
        
        return None
    
    def determine_license_type(self, establishment_name: str) -> str:
        """Determine license type based on establishment name"""
        if not establishment_name:
            return 'other'
            
        name = establishment_name.lower()
        
        if any(word in name for word in ['cultivation', 'grow', 'farm']):
            return 'cultivation'
        elif any(word in name for word in ['dispensary', 'retail', 'store']):
            return 'retail'
        elif any(word in name for word in ['processing', 'extraction', 'manufacturing']):
            return 'processing'
        elif any(word in name for word in ['distribution', 'transport', 'delivery']):
            return 'distribution'
        elif any(word in name for word in ['testing', 'lab', 'laboratory']):
            return 'testing'
        else:
            return 'other'
    
    def is_smoke_shop(self, establishment_name: str) -> bool:
        """Determine if establishment is a smoke shop"""
        if not establishment_name:
            return False
            
        name = establishment_name.lower()
        return any(word in name for word in ['smoke', 'tobacco', 'vape', 'cigar'])
    
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
    
    def create_address_string(self, row) -> str:
        """Create full address string from address components"""
        address_parts = []
        
        # Add street address
        if self.clean_string(row.get('Site Address Line 1')):
            address_parts.append(self.clean_string(row.get('Site Address Line 1')))
        
        # Add city, province, postal code
        city = self.clean_string(row.get('Site City Name'))
        province = self.clean_string(row.get('Site'))  # Assuming this is province/state
        postal = self.clean_string(row.get('Site Postal'))
        
        location_parts = [part for part in [city, province, postal] if part]
        if location_parts:
            address_parts.append(', '.join(location_parts))
        
        return ', '.join(address_parts)
    
    def transform_row_to_schema(self, row) -> Dict[str, Any]:
        """Transform a single Excel row to match the MongoDB schema"""
        
        # Extract basic information
        business_name = self.clean_string(row.get('Establishment Name'))
        license_number = self.clean_string(row.get('Authorization Number'))
        city = self.clean_string(row.get('Site City Name'))
        province = self.clean_string(row.get('Site'))  # Assuming this maps to stateName
        
        # Create business address
        business_address = self.create_address_string(row)
        
        # Parse phone number
        phone = self.parse_phone_number(row.get('Telephone Number'))
        
        # Parse dates
        issue_date = self.parse_date(row.get('Initial Effective'))
        
        # Determine license type and smoke shop status
        license_type = self.determine_license_type(business_name)
        smoke_shop = self.is_smoke_shop(business_name)
        
        # Extract manager/owner information
        manager_name = self.clean_string(row.get('Manager Name'))
        
        # Create the document according to your schema
        document = {
            'business_name': business_name,
            'license_number': license_number,
            'stateName': 'Alberta',  # Assuming 'Site' column contains province/state
            'city': city,
            'business_address': business_address,
            'contact_information': {
                'phone': phone,
                'email': None,  # Not available in Excel data
                'website': None  # Not available in Excel data
            },
            'owner': {
                'name': manager_name,
                'email': None,  # Not available in Excel data
                'role': 'Manager' if manager_name else None,
                'phone': phone,
                'govt_issued_id': None  # Not available in Excel data
            },
            'operator_name': manager_name,
            'issue_date': issue_date,
            'expiration_date': None,  # Not clearly available in Excel data
            'license_type': license_type,
            'license_status': 'Active',  # Assuming active since no status in data
            'jurisdiction': 'Alberta',
            'regulatory_body': 'Health Canada',  # Assuming based on Canadian data
            'entity_type': [license_type],
            'filing_documents_url': None,
            'license_conditions': [],
            'claimed': False,
            'claimedBy': None,
            'claimedAt': None,
            'canojaVerified': True,
            'adminVerificationRequired': False,
            'featured': False,
            'dba': business_name,
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
        
        print(f"Successfully processed {len(documents)} records")
        return documents
    
    def save_to_json(self, documents: List[Dict[str, Any]], output_file: str = 'alberta.json'):
        """Save scraped data to JSON file"""
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
            print(f"City: {doc['city']}, {doc['stateName']}")
            print(f"License Type: {doc['license_type']}")
            print(f"Phone: {doc['contact_information']['phone']}")
            print(f"Issue Date: {doc['issue_date']}")
            print(f"Smoke Shop: {doc['smoke_shop']}")
            print("-" * 50)


# Usage example
if __name__ == "__main__":
    # Initialize scraper with your Excel file path
    scraper = CannabisLicenseScraper('cannabis_search_results.xls')
    
    # Scrape all data
    scraped_data = scraper.scrape_all_data()
    
    if scraped_data:
        # Print sample data for verification
        scraper.print_sample_data(scraped_data, 5)
        
        # Save to JSON file
        scraper.save_to_json(scraped_data)
        
        print(f"\nTotal records processed: {len(scraped_data)}")
        
        # Optional: Print column mapping info
        print("\n=== Column Mapping ===")
        print("Excel Column -> Schema Field")
        print("Authorization Number -> license_number")
        print("Establishment Name -> business_name")
        print("Site City Name -> city")
        print("Site -> stateName")
        print("Manager Name -> owner.name, operator_name")
        print("Initial Effective -> issue_date")
        print("Telephone Number -> contact_information.phone")
        print("Site Address Line 1 -> business_address")
    else:
        print("No data could be processed from the Excel file")


# Additional utility functions for MongoDB operations
def insert_to_mongodb(documents: List[Dict[str, Any]], connection_string: str, database_name: str):
    """
    Insert scraped data into MongoDB
    Requires: pip install pymongo
    """
    try:
        from pymongo import MongoClient
        
        client = MongoClient(connection_string)
        db = client[database_name]
        collection = db['testrecords']  # Adjust collection name as needed
        
        # Insert documents
        result = collection.insert_many(documents)
        print(f"Inserted {len(result.inserted_ids)} documents into MongoDB")
        
        client.close()
    except ImportError:
        print("PyMongo not installed. Run: pip install pymongo")
    except Exception as e:
        print(f"Error inserting to MongoDB: {e}")


scraped_data = scraper.scrape_all_data()
insert_to_mongodb(
    scraped_data, 
    'mongodb://localhost:27017/', 
    'cannabis_licenses'
)