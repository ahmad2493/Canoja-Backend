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
            # Try different Excel formats
            if self.excel_file_path.endswith('.xls'):
                self.df = pd.read_excel(self.excel_file_path, engine='xlrd')
            else:
                self.df = pd.read_excel(self.excel_file_path)
            
            print(f"Loaded {len(self.df)} records from Excel file")
            print(f"Columns found: {list(self.df.columns)}")
            return True
        except Exception as e:
            print(f"Error loading Excel file: {e}")
            return False
    
    def clean_string(self, value) -> Optional[str]:
        """Clean and normalize string values"""
        if pd.isna(value) or value == '' or str(value).lower() in ['none', 'null', 'n/a']:
            return None
        return str(value).strip()
    
    def parse_date(self, date_value) -> Optional[str]:
        """Parse date values and return ISO format string"""
        if pd.isna(date_value) or not date_value:
            return None
            
        try:
            if isinstance(date_value, str):
                # Handle various string date formats
                parsed_date = pd.to_datetime(date_value, errors='coerce')
            else:
                # Handle Excel datetime objects
                parsed_date = pd.to_datetime(date_value)
            
            if pd.isna(parsed_date):
                return None
                
            return parsed_date.isoformat()
        except Exception as e:
            print(f"Date parsing error for value '{date_value}': {e}")
            return None
    
    def determine_license_type(self, facility_type: str, dba: str = None) -> str:
        """Determine license type based on facility type and DBA name"""
        if not facility_type:
            facility_type = ""
        if not dba:
            dba = ""
            
        combined_text = f"{facility_type} {dba}".lower()
        
        # Map based on the facility types visible in your Excel
        if 'cultivation' in combined_text or 'cultivatio' in combined_text:
            return 'cultivation'
        elif any(word in combined_text for word in ['retail', 'dispensary', 'store']):
            return 'retail'
        elif any(word in combined_text for word in ['processing', 'extraction', 'manufacturing', 'infusion']):
            return 'processing'
        elif any(word in combined_text for word in ['distribution', 'transport', 'delivery']):
            return 'distribution'
        elif any(word in combined_text for word in ['testing', 'lab', 'laboratory']):
            return 'testing'
        elif 'medical marijuana' in combined_text:
            return 'medical'
        elif 'optional premises' in combined_text:
            return 'optional_premises'
        else:
            return 'other'
    
    def is_smoke_shop(self, business_name: str, dba: str = None) -> bool:
        """Determine if establishment is a smoke shop"""
        combined_text = f"{business_name or ''} {dba or ''}".lower()
        return any(word in combined_text for word in ['smoke', 'tobacco', 'vape', 'cigar', 'head shop'])
    
    def parse_phone_number(self, phone: str) -> Optional[str]:
        """Clean and format phone numbers"""
        if not phone or pd.isna(phone):
            return None
        
        # Remove all non-digit characters
        digits_only = re.sub(r'\D', '', str(phone))
        
        # Skip if no digits found
        if not digits_only:
            return None
        
        # Format as standard phone number based on length
        if len(digits_only) == 10:
            return f"({digits_only[:3]}) {digits_only[3:6]}-{digits_only[6:]}"
        elif len(digits_only) == 11 and digits_only[0] == '1':
            return f"1-({digits_only[1:4]}) {digits_only[4:7]}-{digits_only[7:]}"
        elif len(digits_only) >= 7:
            # For other lengths, just return the digits
            return digits_only
        
        return None
    
    def create_full_address(self, row) -> str:
        """Create full address string from available address components"""
        address_parts = []
        
        # Get street address
        street = self.clean_string(row.get('Street'))
        if street:
            address_parts.append(street)
        
        # Get city, state/province
        city = self.clean_string(row.get('City'))
        zip_code = self.clean_string(row.get('ZIP Code'))
        
        # Build location part
        location_parts = []
        if city:
            location_parts.append(city)
        if zip_code:
            location_parts.append(zip_code)
            
        if location_parts:
            address_parts.append(', '.join(location_parts))
        
        return ', '.join(address_parts) if address_parts else None
    
    def extract_state_from_data(self, row) -> str:
        """Extract state name - assuming this is Colorado based on your data"""
        # The data appears to be from Colorado based on cities and zip codes
        return 'Colorado'
    
    def get_license_status(self, date_updated: str = None) -> str:
        """Determine license status - assume active if recently updated"""
        if date_updated:
            try:
                update_date = pd.to_datetime(date_updated)
                current_date = pd.Timestamp.now()
                days_since_update = (current_date - update_date).days
                
                # If updated within last 90 days, likely active
                if days_since_update <= 90:
                    return 'Active'
                else:
                    return 'Unknown'
            except:
                pass
        
        return 'Active'  # Default assumption
    
    def transform_row_to_schema(self, row) -> Dict[str, Any]:
        """Transform a single Excel row to match the MongoDB schema"""
        
        # Extract basic information
        license_number = self.clean_string(row.get('License Number'))
        facility_name = self.clean_string(row.get('Facility Name'))
        dba = self.clean_string(row.get('DBA'))
        facility_type = self.clean_string(row.get('Facility Type'))
        city = self.clean_string(row.get('City'))
        date_updated = self.clean_string(row.get('Date Updated'))
        
        # Use facility name as business name, fallback to DBA
        business_name = facility_name or dba
        
        # Create full address
        business_address = self.create_full_address(row)
        
        # Extract state
        state_name = self.extract_state_from_data(row)
        
        # Determine license properties
        license_type = self.determine_license_type(facility_type, dba)
        license_status = self.get_license_status(date_updated)
        smoke_shop = self.is_smoke_shop(business_name, dba)
        
        # Parse dates
        issue_date = self.parse_date(date_updated)  # Using date_updated as proxy for issue_date
        
        # Create the document according to your schema
        document = {
            'googlePlaceId': None,
            'business_name': business_name,
            'license_number': license_number,
            'stateName': state_name,
            'city': city,
            'business_address': business_address,
            'contact_information': {
                'phone': None,  # Not available in this Excel format
                'email': None,
                'website': None
            },
            'owner': {
                'name': None,  # Not available in this Excel format
                'email': None,
                'role': None,
                'phone': None,
                'govt_issued_id': None
            },
            'operator_name': None,  # Not available in this Excel format
            'issue_date': issue_date,
            'expiration_date': None,  # Not available in this Excel format
            'license_type': license_type,
            'license_status': license_status,
            'jurisdiction': state_name,
            'regulatory_body': 'Colorado Department of Revenue',  # Assuming Colorado
            'entity_type': [license_type],
            'filing_documents_url': None,
            'license_conditions': [],
            'claimed': False,
            'claimedBy': None,
            'claimedAt': None,
            'canojaVerified': True,
            'adminVerificationRequired': False,
            'featured': False,
            'dba': dba,
            'state_license_document': None,
            'utility_bill': None,
            'gps_validation': False,
            'location': {
                'type': 'Point',
                'coordinates': []  # Would need geocoding service to populate
            },
            'smoke_shop': smoke_shop
        }
        
        return document
    
    def scrape_all_data(self) -> List[Dict[str, Any]]:
        """Scrape all data from Excel and return list of documents"""
        if not self.load_excel_data():
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
    
    def save_to_json(self, documents: List[Dict[str, Any]], output_file: str = 'colorado.json'):
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
            print(f"Address: {doc['business_address']}")
            print(f"Issue Date: {doc['issue_date']}")
            print(f"License Status: {doc['license_status']}")
            print(f"DBA: {doc['dba']}")
            print(f"Smoke Shop: {doc['smoke_shop']}")
            print("-" * 60)
    
    def print_statistics(self, documents: List[Dict[str, Any]]):
        """Print statistics about the scraped data"""
        if not documents:
            print("No documents to analyze")
            return
            
        print(f"\n=== Data Statistics ===")
        print(f"Total records: {len(documents)}")
        
        # License type distribution
        license_types = {}
        cities = {}
        smoke_shops = 0
        
        for doc in documents:
            # Count license types
            license_type = doc.get('license_type', 'unknown')
            license_types[license_type] = license_types.get(license_type, 0) + 1
            
            # Count cities
            city = doc.get('city', 'unknown')
            cities[city] = cities.get(city, 0) + 1
            
            # Count smoke shops
            if doc.get('smoke_shop'):
                smoke_shops += 1
        
        print(f"\nLicense Types:")
        for license_type, count in sorted(license_types.items()):
            print(f"  {license_type}: {count}")
            
        print(f"\nTop 10 Cities:")
        for city, count in sorted(cities.items(), key=lambda x: x[1], reverse=True)[:10]:
            print(f"  {city}: {count}")
            
        print(f"\nSmoke Shops: {smoke_shops}")


# Usage example and MongoDB integration
def insert_to_mongodb(documents: List[Dict[str, Any]], connection_string: str = 'mongodb://localhost:27017/', 
                     database_name: str = 'cannabis_licenses', collection_name: str = 'testrecords'):
    """
    Insert scraped data into MongoDB
    Requires: pip install pymongo
    """
    try:
        from pymongo import MongoClient
        
        client = MongoClient(connection_string)
        db = client[database_name]
        collection = db[collection_name]
        
        # Clear existing data (optional - remove this line if you want to append)
        # collection.delete_many({})
        
        # Insert documents
        if documents:
            result = collection.insert_many(documents)
            print(f"Inserted {len(result.inserted_ids)} documents into MongoDB")
            
            # Create indexes for better performance
            collection.create_index([("location", "2dsphere")])
            collection.create_index("business_name")
            collection.create_index("license_number")
            collection.create_index("license_status")
            collection.create_index([("city", 1), ("stateName", 1)])
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
    # Initialize scraper with your Excel file path
    excel_file_path = 'Cultivations.xlsx'  # Update this path
    scraper = CannabisLicenseScraper(excel_file_path)
    
    # Scrape all data
    print("Starting data extraction...")
    scraped_data = scraper.scrape_all_data()
    
    if scraped_data:
        # Print sample data for verification
        scraper.print_sample_data(scraped_data, 5)
        
        # Print statistics
        scraper.print_statistics(scraped_data)
        
        # Save to JSON file
        scraper.save_to_json(scraped_data)
        
        # Optional: Insert to MongoDB (uncomment to use)
        print("\nInserting data to MongoDB...")
        insert_to_mongodb(scraped_data)
        
        print(f"\n✅ Processing complete! Total records processed: {len(scraped_data)}")
        
    else:
        print("❌ No data could be processed from the Excel file")
        print("Please check the file path and format")

"""
Excel Column Mappings:
- License Number -> license_number
- Facility Name -> business_name  
- DBA -> dba
- Facility Type -> license_type (processed)
- Street -> business_address (part of)
- City -> city
- ZIP Code -> business_address (part of)  
- Date Updated -> issue_date (as proxy)

Note: Some fields like phone, email, owner info are not available 
in this Excel format and will be set to None/null.
"""