import json
import pandas as pd
from datetime import datetime
import re
from typing import Dict, List, Optional, Any

class OntarioCannabisLicenseScraper:
    def __init__(self, json_file_path: str):
        self.json_file_path = json_file_path
        self.data = None
        
    def load_json_data(self):
        """Load JSON data from ESRI ArcGIS format"""
        try:
            with open(self.json_file_path, 'r', encoding='utf-8') as f:
                raw_data = json.load(f)
            
            # Extract features array from ESRI format
            if 'features' in raw_data:
                self.data = raw_data['features']
                print(f"Loaded {len(self.data)} records from ESRI JSON file")
                
                # Print sample record structure
                if self.data and len(self.data) > 0:
                    sample_record = self.data[0]
                    print("Sample record structure:")
                    if 'attributes' in sample_record:
                        print(f"  Attributes: {list(sample_record['attributes'].keys())}")
                    if 'geometry' in sample_record:
                        print(f"  Geometry: {sample_record['geometry'].keys()}")
                return True
            else:
                print("No 'features' array found in JSON file")
                return False
                
        except Exception as e:
            print(f"Error loading JSON file: {e}")
            return False
    
    def clean_string(self, value) -> Optional[str]:
        """Clean and normalize string values"""
        if value is None or value == '' or str(value).lower() in ['none', 'null', 'n/a', 'nan', '.']:
            return None
        return str(value).strip()
    
    def parse_date(self, date_value) -> Optional[str]:
        """Parse date values and return ISO format string"""
        if not date_value or date_value in [None, '', 'null', 'None', '.']:
            return None
            
        try:
            if isinstance(date_value, str):
                # Handle various string date formats
                parsed_date = pd.to_datetime(date_value, errors='coerce')
            else:
                # Handle datetime objects
                parsed_date = pd.to_datetime(date_value)
            
            if pd.isna(parsed_date):
                return None
                
            return parsed_date.isoformat()
        except Exception as e:
            print(f"Date parsing error for value '{date_value}': {e}")
            return None
    
    def determine_license_type(self, premises_name: str) -> str:
        """Determine license type based on premises name"""
        if not premises_name:
            return 'retail'  # Default for Ontario cannabis stores
            
        name_lower = premises_name.lower()
        
        # Most Ontario entries appear to be retail stores
        # Look for specific indicators
        if any(word in name_lower for word in ['cultivation', 'cultivator', 'grow', 'farm']):
            return 'cultivation'
        elif any(word in name_lower for word in ['processing', 'processor', 'extraction', 'manufacturing']):
            return 'processing'
        elif any(word in name_lower for word in ['distribution', 'distributor', 'transport', 'delivery']):
            return 'distribution'
        elif any(word in name_lower for word in ['testing', 'lab', 'laboratory']):
            return 'testing'
        else:
            # Default to retail for Ontario cannabis stores
            return 'retail'
    
    def is_smoke_shop(self, business_name: str) -> bool:
        """Determine if establishment is a smoke shop"""
        if not business_name:
            return False
            
        name_lower = business_name.lower()
        return any(word in name_lower for word in ['smoke', 'tobacco', 'vape', 'cigar', 'head shop'])
    
    def get_license_status(self, application_status: str) -> str:
        """Map application status to license status"""
        if not application_status:
            return 'Active'
            
        status_lower = application_status.lower()
        if 'authorized to open' in status_lower:
            return 'Active'
        elif 'pending' in status_lower:
            return 'Pending'
        elif 'denied' in status_lower or 'rejected' in status_lower:
            return 'Denied'
        elif 'suspended' in status_lower:
            return 'Suspended'
        else:
            return 'Active'  # Default
    
    def extract_coordinates(self, geometry: Dict) -> List[float]:
        """Extract coordinates from ESRI geometry"""
        if not geometry or 'x' not in geometry or 'y' not in geometry:
            return []
        
        try:
            # ESRI format uses Web Mercator (3857), need to convert to WGS84 if needed
            # For now, we'll assume the lat/lon in attributes are correct
            x = float(geometry['x'])
            y = float(geometry['y'])
            return [x, y]  # [longitude, latitude] format for GeoJSON
        except (ValueError, TypeError):
            return []
    
    def transform_record_to_schema(self, record: Dict) -> Dict[str, Any]:
        """Transform a single ESRI JSON record to match the MongoDB schema"""
        
        # Extract attributes and geometry
        attributes = record.get('attributes', {})
        geometry = record.get('geometry', {})
        
        # Extract basic information from attributes
        premises_name = self.clean_string(attributes.get('PremisesName'))
        street_address = self.clean_string(attributes.get('StreetAddress'))
        city = self.clean_string(attributes.get('City'))
        province = self.clean_string(attributes.get('Province'))
        postal_code = self.clean_string(attributes.get('PostalCode'))
        application_status = self.clean_string(attributes.get('ApplicationStatus'))
        website = self.clean_string(attributes.get('Website'))
        public_notice_date = self.clean_string(attributes.get('PublicNoticeDate'))
        
        # Build full address
        address_parts = []
        if street_address:
            address_parts.append(street_address)
        
        location_parts = []
        if city:
            location_parts.append(city)
        if province:
            location_parts.append(province)
        if postal_code:
            location_parts.append(postal_code)
            
        if location_parts:
            address_parts.append(', '.join(location_parts))
        
        business_address = ', '.join(address_parts) if address_parts else None
        
        # Extract coordinates
        latitude = attributes.get('Latitude')
        longitude = attributes.get('Longitude')
        coordinates = []
        if longitude and latitude:
            try:
                coordinates = [float(longitude), float(latitude)]
            except (ValueError, TypeError):
                coordinates = []
        
        # Determine license properties
        license_type = self.determine_license_type(premises_name)
        license_status = self.get_license_status(application_status)
        smoke_shop = self.is_smoke_shop(premises_name)
        
        # Parse date
        issue_date = self.parse_date(public_notice_date) if public_notice_date and public_notice_date != '.' else None
        
        # Create the document according to your schema
        document = {
            'googlePlaceId': None,
            'business_name': premises_name,
            'license_number': None,  # Not available in this dataset
            'stateName': 'Ontario',
            'city': city,
            'business_address': business_address,
            'contact_information': {
                'phone': None,  # Not available in this dataset
                'email': None,  # Not available in this dataset
                'website': website
            },
            'owner': {
                'name': None,  # Not available in this dataset
                'email': None,
                'role': None,
                'phone': None,
                'govt_issued_id': None
            },
            'operator_name': None,  # Not available in this dataset
            'issue_date': issue_date,
            'expiration_date': None,  # Not available in this dataset
            'license_type': license_type,
            'license_status': license_status,
            'jurisdiction': 'Ontario',
            'regulatory_body': 'Alcohol and Gaming Commission of Ontario (AGCO)',
            'entity_type': [license_type],
            'filing_documents_url': self.clean_string(attributes.get('URLLink')),
            'license_conditions': [],
            'claimed': False,
            'claimedBy': None,
            'claimedAt': None,
            'canojaVerified': True,
            'adminVerificationRequired': False,
            'featured': False,
            'dba': None,  # Not available in this dataset
            'state_license_document': None,
            'utility_bill': None,
            'gps_validation': bool(coordinates),
            'location': {
                'type': 'Point',
                'coordinates': coordinates
            },
            'smoke_shop': smoke_shop,
            'application_status': application_status,  # Additional field specific to this dataset
            'objectid': attributes.get('OBJECTID'),  # Keep original ID for reference
            'postal_code': postal_code
        }
        
        return document
    
    def scrape_all_data(self) -> List[Dict[str, Any]]:
        """Process all data from JSON and return list of documents"""
        if not self.load_json_data():
            return []
        
        documents = []
        processed_count = 0
        error_count = 0
        
        for index, record in enumerate(self.data):
            try:
                document = self.transform_record_to_schema(record)
                
                # Only include records with essential data (business name is required)
                if document['business_name']:
                    documents.append(document)
                    processed_count += 1
                else:
                    print(f"Skipping record {index + 1}: Missing business name")
                    
            except Exception as e:
                print(f"Error processing record {index + 1}: {e}")
                error_count += 1
                continue
        
        print(f"Successfully processed {processed_count} records")
        print(f"Errors encountered: {error_count}")
        return documents
    
    def save_to_json(self, documents: List[Dict[str, Any]], output_file: str = 'ontario_processed.json'):
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
            print(f"City: {doc['city']}, {doc['stateName']}")
            print(f"License Type: {doc['license_type']}")
            print(f"Application Status: {doc['application_status']}")
            print(f"Address: {doc['business_address']}")
            print(f"Website: {doc['contact_information']['website']}")
            print(f"License Status: {doc['license_status']}")
            print(f"Coordinates: {doc['location']['coordinates']}")
            print(f"Object ID: {doc['objectid']}")
            print(f"Smoke Shop: {doc['smoke_shop']}")
            print("-" * 60)
    
    def print_statistics(self, documents: List[Dict[str, Any]]):
        """Print statistics about the scraped data"""
        if not documents:
            print("No documents to analyze")
            return
            
        print(f"\n=== Data Statistics ===")
        print(f"Total records: {len(documents)}")
        
        # Various statistics
        license_types = {}
        cities = {}
        statuses = {}
        application_statuses = {}
        smoke_shops = 0
        with_coordinates = 0
        with_websites = 0
        
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
            
            # Count application statuses
            app_status = doc.get('application_status', 'unknown')
            application_statuses[app_status] = application_statuses.get(app_status, 0) + 1
            
            # Count smoke shops
            if doc.get('smoke_shop'):
                smoke_shops += 1
                
            # Count records with coordinates
            if doc.get('location', {}).get('coordinates'):
                with_coordinates += 1
                
            # Count records with websites
            if doc.get('contact_information', {}).get('website'):
                with_websites += 1
        
        print(f"\nLicense Types:")
        for license_type, count in sorted(license_types.items()):
            print(f"  {license_type}: {count}")
            
        print(f"\nLicense Statuses:")
        for status, count in sorted(statuses.items()):
            print(f"  {status}: {count}")
            
        print(f"\nApplication Statuses:")
        for app_status, count in sorted(application_statuses.items()):
            print(f"  {app_status}: {count}")
            
        print(f"\nTop 15 Cities:")
        for city, count in sorted(cities.items(), key=lambda x: x[1], reverse=True)[:15]:
            print(f"  {city}: {count}")
            
        print(f"\nOther Statistics:")
        print(f"  Records with coordinates: {with_coordinates}")
        print(f"  Records with websites: {with_websites}")
        print(f"  Smoke shops: {smoke_shops}")


# MongoDB integration
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
        
        # Insert documents
        if documents:
            # Clear existing data (optional - remove this line if you want to append)
            # collection.delete_many({})
            
            result = collection.insert_many(documents)
            print(f"Inserted {len(result.inserted_ids)} documents into MongoDB")
            
            # Create indexes for better performance
            collection.create_index([("location", "2dsphere")])
            collection.create_index("business_name")
            collection.create_index("license_number")
            collection.create_index("license_status")
            collection.create_index("application_status")
            collection.create_index([("city", 1), ("stateName", 1)])
            collection.create_index("smoke_shop")
            collection.create_index("license_type")
            collection.create_index("objectid")
            
            print("Created database indexes")
        else:
            print("No documents to insert")
        
        client.close()
    except ImportError:
        print("PyMongo not installed. Run: pip install pymongo")
    except Exception as e:
        print(f"Error inserting to MongoDB: {e}")


if __name__ == "__main__":
    # Initialize scraper with your JSON file path
    json_file_path = 'ontario.json'  # Update this path if needed
    scraper = OntarioCannabisLicenseScraper(json_file_path)
    
    # Process all data
    print("Starting data extraction from ESRI JSON...")
    scraped_data = scraper.scrape_all_data()
    
    if scraped_data:
        # Print sample data for verification
        scraper.print_sample_data(scraped_data, 5)
        
        # Print statistics
        scraper.print_statistics(scraped_data)
        
        # Save to processed JSON file (optional)
        scraper.save_to_json(scraped_data, 'ontario_processed.json')
        
        # Insert to MongoDB
        print("\nInserting data to MongoDB...")
        insert_to_mongodb(scraped_data)
        
        print(f"\n✅ Processing complete! Total records processed: {len(scraped_data)}")
        
    else:
        print("❌ No data could be processed from the JSON file")
        print("Please check the file path and format")

"""
This script processes Ontario cannabis license data from ESRI ArcGIS JSON format and stores it in MongoDB.

The script handles the specific structure of your JSON:
- Extracts data from 'features' array
- Maps 'attributes' to business information
- Extracts coordinates from geometry
- Maps ApplicationStatus to license status

Required dependencies:
pip install pymongo pandas

The script will:
1. Read ontario.json file (ESRI ArcGIS format)
2. Transform data to match your MongoDB schema
3. Store in MongoDB collection 'ontario_records'
4. Create appropriate database indexes
5. Print statistics and sample data

Key fields mapped:
- PremisesName -> business_name
- StreetAddress, City, Province, PostalCode -> business_address
- ApplicationStatus -> license_status
- Website -> contact_information.website
- Latitude, Longitude -> location.coordinates
- OBJECTID -> objectid (for reference)
"""