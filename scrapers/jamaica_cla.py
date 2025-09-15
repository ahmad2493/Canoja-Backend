import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
from datetime import datetime
import re
from typing import Dict, List, Optional, Any
import time

class JamaicaCLALicenseScraper:
    def __init__(self, base_url: str = "https://cla.org.jm/cla-licensees/"):
        self.base_url = base_url
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        
    def fetch_page(self, url: str) -> Optional[BeautifulSoup]:
        """Fetch a single page and return BeautifulSoup object"""
        try:
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            return BeautifulSoup(response.content, 'html.parser')
        except Exception as e:
            print(f"Error fetching {url}: {e}")
            return None
    
    def parse_date(self, date_str: str) -> Optional[str]:
        """Parse date string and return ISO format"""
        if not date_str or pd.isna(date_str):
            return None
            
        try:
            # Parse date format like "2025-01-12"
            parsed_date = pd.to_datetime(date_str, errors='coerce')
            if pd.isna(parsed_date):
                return None
            return parsed_date.isoformat()
        except:
            return None
    
    def clean_string(self, value) -> Optional[str]:
        """Clean and normalize string values"""
        if not value or pd.isna(value):
            return None
        return str(value).strip()
    
    def determine_license_type(self, license_type_str: str) -> str:
        """Map Jamaica license types to our schema"""
        if not license_type_str:
            return 'other'
            
        license_type = license_type_str.lower()
        
        # Map Jamaica license types
        if 'retail' in license_type and 'therapeutic' in license_type:
            return 'retail'
        elif 'retail' in license_type and 'herb house' in license_type:
            return 'retail'
        elif 'processing' in license_type:
            return 'processing'
        elif 'cultivator' in license_type or 'cultivation' in license_type:
            return 'cultivation'
        elif 'transport' in license_type:
            return 'distribution'
        else:
            return 'retail'  # Default for most Jamaica licenses
    
    def extract_city_from_address(self, address: str) -> Optional[str]:
        """Extract city/parish from Jamaica address"""
        if not address:
            return None
            
        # Jamaica parishes that might appear in addresses
        parishes = [
            'Kingston', 'St. Andrew', 'Saint Andrew', 'St Andrew',
            'St. Catherine', 'Saint Catherine', 'St Catherine',
            'Clarendon', 'Manchester', 'Westmoreland', 'St. James', 
            'Saint James', 'St James', 'Hanover', 'Trelawny',
            'St. Ann', 'Saint Ann', 'St Ann', 'St. Mary', 'Saint Mary', 'St Mary',
            'Portland', 'St. Thomas', 'Saint Thomas', 'St Thomas'
        ]
        
        address_lower = address.lower()
        for parish in parishes:
            if parish.lower() in address_lower:
                return parish
        
        # Try to extract from common patterns
        if 'p.o.' in address_lower:
            # Look for parish after P.O.
            parts = address.split(',')
            for part in parts:
                part = part.strip()
                if any(parish.lower() in part.lower() for parish in parishes):
                    return part
        
        return None
    
    def scrape_table_data(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        """Extract data from the HTML table"""
        licenses = []
        
        # Find the table with license data
        table = soup.find('table')
        if not table:
            print("No table found on the page")
            return licenses
        
        # Find all rows (skip header)
        rows = table.find_all('tr')[1:]  # Skip header row
        
        for row in rows:
            cells = row.find_all('td')
            if len(cells) >= 4:  # Ensure we have all required columns
                try:
                    licensee = self.clean_string(cells[0].get_text())
                    license_type = self.clean_string(cells[1].get_text())
                    business_address = self.clean_string(cells[2].get_text())
                    expiry_date = self.clean_string(cells[3].get_text())
                    
                    # Extract city/parish from address
                    city = self.extract_city_from_address(business_address)
                    
                    # Determine our license type
                    mapped_license_type = self.determine_license_type(license_type)
                    
                    # Parse expiry date
                    parsed_expiry = self.parse_date(expiry_date)
                    
                    # Create document according to MongoDB schema
                    document = {
                        'business_name': licensee,
                        'license_number': None,  # Not provided in the data
                        'stateName': 'Jamaica',
                        'city': city,
                        'business_address': business_address,
                        'contact_information': {
                            'phone': None,
                            'email': None,
                            'website': None
                        },
                        'owner': {
                            'name': licensee,  # Using business name as owner
                            'email': None,
                            'role': 'Owner',
                            'phone': None,
                            'govt_issued_id': None
                        },
                        'operator_name': licensee,
                        'issue_date': None,  # Not provided
                        'expiration_date': parsed_expiry,
                        'license_type': mapped_license_type,
                        'license_status': 'Active',  # Assuming active since listed
                        'jurisdiction': 'Jamaica',
                        'regulatory_body': 'Cannabis Licensing Authority (CLA)',
                        'entity_type': [mapped_license_type],
                        'filing_documents_url': None,
                        'license_conditions': [],
                        'claimed': False,
                        'claimedBy': None,
                        'claimedAt': None,
                        'canojaVerified': True,
                        'adminVerificationRequired': False,
                        'featured': False,
                        'dba': licensee,
                        'state_license_document': None,
                        'utility_bill': None,
                        'gps_validation': False,
                        'location': {
                            'type': 'Point',
                            'coordinates': []  # Would need geocoding
                        },
                        'smoke_shop': False,  # These are cannabis licenses, not smoke shops
                        'original_license_type': license_type  # Keep original for reference
                    }
                    
                    licenses.append(document)
                    
                except Exception as e:
                    print(f"Error processing row: {e}")
                    continue
        
        return licenses
    
    def scrape_all_pages(self) -> List[Dict[str, Any]]:
        """Scrape all pages of license data"""
        all_licenses = []
        page_num = 1
        
        while True:
            print(f"Scraping page {page_num}...")
            
            # For the first page, use base URL, for others add page parameter
            if page_num == 1:
                url = self.base_url
            else:
                # You may need to adjust this based on the actual pagination structure
                url = f"{self.base_url}?page={page_num}"
            
            soup = self.fetch_page(url)
            if not soup:
                print(f"Failed to fetch page {page_num}")
                break
            
            page_licenses = self.scrape_table_data(soup)
            
            if not page_licenses:
                print(f"No licenses found on page {page_num}, stopping.")
                break
            
            all_licenses.extend(page_licenses)
            print(f"Found {len(page_licenses)} licenses on page {page_num}")
            
            # Check if there's a next page
            # This may need adjustment based on actual pagination structure
            next_link = soup.find('a', text=re.compile(r'next|›|»', re.I))
            if not next_link:
                print("No more pages found.")
                break
            
            page_num += 1
            time.sleep(1)  # Be respectful to the server
            
            # Safety check to avoid infinite loops
            if page_num > 50:
                print("Reached maximum page limit (50), stopping.")
                break
        
        return all_licenses
    
    def save_to_json(self, licenses: List[Dict[str, Any]], filename: str = 'jamaica_cla.json'):
        """Save licenses to JSON file"""
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(licenses, f, indent=2, ensure_ascii=False, default=str)
            print(f"Saved {len(licenses)} licenses to {filename}")
        except Exception as e:
            print(f"Error saving to JSON: {e}")
    
    def save_to_csv(self, licenses: List[Dict[str, Any]], filename: str = 'jamaica_cla_licenses.csv'):
        """Save licenses to CSV file"""
        try:
            # Flatten the nested structure for CSV
            flattened_data = []
            for license in licenses:
                flat_record = {
                    'business_name': license['business_name'],
                    'license_number': license['license_number'],
                    'state_name': license['stateName'],
                    'city': license['city'],
                    'business_address': license['business_address'],
                    'phone': license['contact_information']['phone'],
                    'email': license['contact_information']['email'],
                    'website': license['contact_information']['website'],
                    'owner_name': license['owner']['name'],
                    'operator_name': license['operator_name'],
                    'issue_date': license['issue_date'],
                    'expiration_date': license['expiration_date'],
                    'license_type': license['license_type'],
                    'license_status': license['license_status'],
                    'jurisdiction': license['jurisdiction'],
                    'regulatory_body': license['regulatory_body'],
                    'original_license_type': license.get('original_license_type', '')
                }
                flattened_data.append(flat_record)
            
            df = pd.DataFrame(flattened_data)
            df.to_csv(filename, index=False)
            print(f"Saved {len(licenses)} licenses to {filename}")
        except Exception as e:
            print(f"Error saving to CSV: {e}")
    
    def print_summary_stats(self, licenses: List[Dict[str, Any]]):
        """Print summary statistics"""
        print(f"\n=== Jamaica CLA License Summary ===")
        print(f"Total Licenses: {len(licenses)}")
        
        # Count by license type
        license_types = {}
        cities = {}
        original_types = {}
        
        for license in licenses:
            lt = license['license_type']
            license_types[lt] = license_types.get(lt, 0) + 1
            
            city = license.get('city', 'Unknown')
            cities[city] = cities.get(city, 0) + 1
            
            orig_type = license.get('original_license_type', 'Unknown')
            original_types[orig_type] = original_types.get(orig_type, 0) + 1
        
        print(f"\nMapped License Types:")
        for lt, count in sorted(license_types.items()):
            print(f"  {lt}: {count}")
        
        print(f"\nOriginal License Types:")
        for lt, count in sorted(original_types.items(), key=lambda x: x[1], reverse=True):
            print(f"  {lt}: {count}")
        
        print(f"\nTop Parishes/Cities:")
        for city, count in sorted(cities.items(), key=lambda x: x[1], reverse=True)[:10]:
            print(f"  {city or 'Unknown'}: {count}")
    
    def print_sample_data(self, licenses: List[Dict[str, Any]], num_samples: int = 3):
        """Print sample data for verification"""
        print(f"\n=== Sample of {min(num_samples, len(licenses))} records ===")
        for i, license in enumerate(licenses[:num_samples]):
            print(f"\nRecord {i + 1}:")
            print(f"Business Name: {license['business_name']}")
            print(f"Original License Type: {license.get('original_license_type')}")
            print(f"Mapped License Type: {license['license_type']}")
            print(f"City/Parish: {license['city']}")
            print(f"Address: {license['business_address']}")
            print(f"Expiration Date: {license['expiration_date']}")
            print("-" * 60)


# Alternative: Manual data entry from screenshots
def create_jamaica_data_from_screenshots():
    """Create data based on the screenshots provided"""
    licenses_data = [
        {
            'licensee': 'Apollon Formularies Jamaica Limited',
            'license_type': 'Retail (Therapeutic Services)',
            'business_address': 'Volume 1526 Folio 184, 1 1A & 2, West End Road, Negril P.O. in the parish of Westmoreland',
            'expiry_date': '2025-01-12'
        },
        {
            'licensee': 'Apollon Formularies Jamaica Limited',
            'license_type': 'Processing (Tier 1)',
            'business_address': 'Volume 1526 Folio 184, 1 1A & 2, West End Road, Negril P.O. in the parish of Westmoreland',
            'expiry_date': '2025-01-12'
        },
        {
            'licensee': 'Castleblack Cannabis Limited',
            'license_type': 'Transport',
            'business_address': 'Lot 104 Nutfield, Islington P.O., St. Mary',
            'expiry_date': '2025-02-14'
        },
        {
            'licensee': 'Haslam HG Enterprize Limited',
            'license_type': 'Transport',
            'business_address': '',
            'expiry_date': '2025-09-13'
        },
        {
            'licensee': 'Haslam HG Enterprize Limited',
            'license_type': 'Retail (Herb House-Without Facilities for Consumption) Licence',
            'business_address': 'Volume 1491 Folio 37, Lot/Apt 1+, Shop No. 14, 12A Molynes Road, Kingston 10, Saint Andrew',
            'expiry_date': '2026-02-01'
        },
        {
            'licensee': 'Hedoweedo Limited',
            'license_type': 'Retail (Herb House) Licence',
            'business_address': 'Rutland Point, Negril P.O., Westmoreland',
            'expiry_date': '2026-06-29'
        },
        {
            'licensee': 'Island Kaya Farms',
            'license_type': "Cultivator's (Tier 2) Licence",
            'business_address': "Greenwich Park, St Ann's Bay P.O., in the parish of Saint Ann",
            'expiry_date': '2025-03-23'
        },
        {
            'licensee': 'Island Strains',
            'license_type': 'Retail (Herb House) Licence',
            'business_address': 'Casa Blanca Hotel, Jimmy Cliff Boulevard, White Sands Beach P.O., Montego Bay, St. James',
            'expiry_date': '2024-10-14'
        },
        {
            'licensee': 'Kaya Extracts Limited',
            'license_type': 'Processing (Tier 1) Licence',
            'business_address': "Greenwich Park, Saint Ann's Bay P.O., in the parish of Saint Ann",
            'expiry_date': '2026-01-18'
        },
        {
            'licensee': '"4/20 Ganjabliss Dispensary"',
            'license_type': 'Retail (Herb House-Without Facilities for Consumption)',
            'business_address': 'Volume 1454 Folio 933, Cave Hall Pen, Discovery Bay P.O., in the parish of Saint Ann',
            'expiry_date': '2027-04-18'
        },
        {
            'licensee': '"4/20 Therapeutic Bliss"',
            'license_type': 'Retail (Herb House-Without Facilities for Consumption)',
            'business_address': 'Volume 1540 Folio 666, Lot 1 Beverley, Runaway Bay P.O., in the parish of Saint Ann',
            'expiry_date': '2027-01-15'
        },
        {
            'licensee': 'Mez Herbal Jamaica Limited',
            'license_type': 'Retail (Herb House-Without Facilities for Consumption)',
            'business_address': 'Shop 19, Orchid Village, 20 Barbican Road, Kingston 6, St. Andrew.',
            'expiry_date': '2024-09-16'
        },
        {
            'licensee': 'Mr. P Tuff Buds',
            'license_type': 'Retail (Herb House-Without Facilities for Consumption)',
            'business_address': 'Volume 1502 Folio 391, Shop No. 29 Washington Plaza Lot 1+Patrick City, 26-32 Auburn Terrace, Kingston 20, Saint Andrew',
            'expiry_date': '2027-04-24'
        },
        {
            'licensee': 'Outlier Bio-Pharma Limited',
            'license_type': 'Processing (Tier 1) Licence',
            'business_address': 'Shop 41 Blue Diamond Shopping Centre, Lot/Apt 1+ Ironshore, Morgan Road, Little River P.O., Montego Bay, in the parish of St. James',
            'expiry_date': '2026-04-20'
        },
        {
            'licensee': 'Outlier Bio-Pharma Limited',
            'license_type': 'Retail (Herb House-Without Facilities for Consumption) Licence',
            'business_address': 'Shop EU7 Whitter Village Centre, Lot/Apt. 1 Ironshore Section 2A, Little River P.O., Montego Bay, in the parish of St. James',
            'expiry_date': '2026-04-20'
        },
        {
            'licensee': 'Target Agricultural Corp Limited',
            'license_type': 'Cultivator (Tier 1)',
            'business_address': 'Valutation No. 083-05-003-078, Craig Head, Craig Head P.O. Manchester',
            'expiry_date': '2025-05-30'
        }
    ]
    
    scraper = JamaicaCLALicenseScraper()
    processed_licenses = []
    
    for data in licenses_data:
        city = scraper.extract_city_from_address(data['business_address'])
        mapped_type = scraper.determine_license_type(data['license_type'])
        
        document = {
            'business_name': data['licensee'].replace('"', ''),
            'license_number': None,
            'stateName': 'Jamaica',
            'city': city,
            'business_address': data['business_address'],
            'contact_information': {
                'phone': None,
                'email': None,
                'website': None
            },
            'owner': {
                'name': data['licensee'].replace('"', ''),
                'email': None,
                'role': 'Owner',
                'phone': None,
                'govt_issued_id': None
            },
            'operator_name': data['licensee'].replace('"', ''),
            'issue_date': None,
            'expiration_date': scraper.parse_date(data['expiry_date']),
            'license_type': mapped_type,
            'license_status': 'Active',
            'jurisdiction': 'Jamaica',
            'regulatory_body': 'Cannabis Licensing Authority (CLA)',
            'entity_type': [mapped_type],
            'filing_documents_url': None,
            'license_conditions': [],
            'claimed': False,
            'claimedBy': None,
            'claimedAt': None,
            'canojaVerified': True,
            'adminVerificationRequired': False,
            'featured': False,
            'dba': data['licensee'].replace('"', ''),
            'state_license_document': None,
            'utility_bill': None,
            'gps_validation': False,
            'location': {
                'type': 'Point',
                'coordinates': []
            },
            'smoke_shop': False,
            'original_license_type': data['license_type']
        }
        processed_licenses.append(document)
    
    return processed_licenses

# MongoDB insertion function
def insert_to_mongodb(documents: List[Dict[str, Any]], connection_string: str, database_name: str, collection_name: str = 'testrecords'):
    """Insert Jamaica license data into MongoDB"""
    try:
        from pymongo import MongoClient
        
        client = MongoClient(connection_string)
        db = client[database_name]
        collection = db[collection_name]
        
        result = collection.insert_many(documents)
        print(f"Inserted {len(result.inserted_ids)} Jamaica licenses into MongoDB")
        
        client.close()
    except ImportError:
        print("PyMongo not installed. Run: pip install pymongo")
    except Exception as e:
        print(f"Error inserting to MongoDB: {e}")
        
# Usage example
if __name__ == "__main__":
    # Option 1: Web scraping (if the site allows it)
    scraper = JamaicaCLALicenseScraper()
    
    try:
        print("Attempting to scrape Jamaica CLA website...")
        licenses = scraper.scrape_all_pages()
        
        if not licenses:
            print("Web scraping failed or no data found. Using screenshot data...")
            licenses = create_jamaica_data_from_screenshots()
    except Exception as e:
        print(f"Web scraping failed: {e}")
        print("Using screenshot data...")
        licenses = create_jamaica_data_from_screenshots()
    
    if licenses:
        # Print sample data
        scraper.print_sample_data(licenses, 5)
        
        # Print summary statistics
        scraper.print_summary_stats(licenses)
        
        # Save data
        scraper.save_to_json(licenses)
        scraper.save_to_csv(licenses)
        
        print(f"\nTotal records processed: {len(licenses)}")
        insert_to_mongodb(
        licenses,
        'mongodb://localhost:27017/',
        'cannabis_licenses',
        'testrecords' 
    )
    else:
        print("No data could be processed")


