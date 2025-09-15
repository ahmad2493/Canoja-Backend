import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
import re
from datetime import datetime
from typing import Dict, List, Optional, Any
import time

class HealthCanadaLicenseScraper:
    def __init__(self, url: str = None):
        self.url = url or "https://www.canada.ca/en/health-canada/services/drugs-medication/cannabis/industry-licensees-applicants/licensed-cultivators-processors-sellers.html"
        self.data = []
        
    def fetch_page_content(self) -> Optional[BeautifulSoup]:
        """Fetch the webpage content"""
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            
            print(f"Fetching data from: {self.url}")
            response = requests.get(self.url, headers=headers, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            print("✓ Successfully fetched webpage content")
            return soup
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching webpage: {e}")
            return None
    
    def parse_license_types(self, license_cell_text: str) -> List[str]:
        """Parse license types from the license column"""
        if not license_cell_text:
            return []
        
        license_types = []
        text = license_cell_text.lower()
        
        # Extract different license types
        if 'micro-processing' in text:
            license_types.append('micro-processing')
        elif 'processing' in text:
            license_types.append('processing')
            
        if 'micro-cultivation' in text:
            license_types.append('micro-cultivation')
        elif 'cultivation' in text:
            license_types.append('cultivation')
            
        if 'sale' in text:
            license_types.append('retail')
            
        return license_types if license_types else ['other']
    
    def parse_authorized_products(self, products_text: str) -> List[str]:
        """Parse authorized products from the products column"""
        if not products_text:
            return []
        
        products = []
        text = products_text.lower()
        
        if 'plants' in text or 'seeds' in text:
            products.append('plants_seeds')
        if 'dried' in text or 'fresh' in text:
            products.append('dried_fresh')
        if 'extracts' in text:
            products.append('extracts')
        if 'edible' in text:
            products.append('edibles')
        if 'topical' in text:
            products.append('topicals')
            
        return products
    
    def clean_phone_number(self, phone: str) -> Optional[str]:
        """Clean and format phone numbers"""
        if not phone or phone.lower() in ['n/a', 'none']:
            return None
        
        # Remove all non-digit characters
        digits_only = re.sub(r'\D', '', str(phone))
        
        # Format as standard phone number if we have 10 digits
        if len(digits_only) == 10:
            return f"({digits_only[:3]}) {digits_only[3:6]}-{digits_only[6:]}"
        elif len(digits_only) == 11 and digits_only[0] == '1':
            return f"1-({digits_only[1:4]}) {digits_only[4:7]}-{digits_only[7:]}"
        
        return digits_only if digits_only else None
    
    def parse_date(self, date_str: str) -> Optional[str]:
        """Parse date from YYYY-MM-DD format"""
        if not date_str or date_str.lower() in ['n/a', 'none']:
            return None
            
        try:
            # Handle YYYY-MM-DD format
            if re.match(r'\d{4}-\d{2}-\d{2}', date_str):
                parsed_date = datetime.strptime(date_str, '%Y-%m-%d')
                return parsed_date.isoformat()
        except:
            return None
        
        return None
    
    def determine_license_status(self, license_text: str) -> str:
        """Determine license status from license text"""
        if not license_text:
            return 'Unknown'
        
        text = license_text.lower()
        if 'revoked' in text:
            return 'Revoked'
        else:
            return 'Active'
    
    def scrape_table_data(self) -> List[Dict[str, Any]]:
        """Scrape data from the Health Canada table"""
        soup = self.fetch_page_content()
        if not soup:
            return []
        
        # Find the table (it might be in different locations)
        table = soup.find('table')
        if not table:
            print("Could not find the main table on the page")
            return []
        
        print("✓ Found table, parsing data...")
        
        # Get table headers
        headers = []
        header_row = table.find('thead')
        if header_row:
            for th in header_row.find_all('th'):
                headers.append(th.get_text(strip=True))
        else:
            # If no thead, try first tr
            first_row = table.find('tr')
            if first_row:
                for th in first_row.find_all(['th', 'td']):
                    headers.append(th.get_text(strip=True))
        
        print(f"Table headers found: {headers}")
        
        # Get table data
        tbody = table.find('tbody') or table
        rows = tbody.find_all('tr')
        
        # Skip header row if it's part of tbody
        if not table.find('thead') and rows:
            rows = rows[1:]
        
        scraped_data = []
        
        for i, row in enumerate(rows):
            cells = row.find_all(['td', 'th'])
            if len(cells) < 4:  # Skip rows that don't have enough data
                continue
                
            try:
                # Extract data based on expected column positions
                license_holder = cells[0].get_text(strip=True) if len(cells) > 0 else ""
                province = cells[1].get_text(strip=True) if len(cells) > 1 else ""
                licenses = cells[2].get_text(strip=True) if len(cells) > 2 else ""
                authorized_products = cells[3].get_text(strip=True) if len(cells) > 3 else ""
                registered_patients = cells[4].get_text(strip=True) if len(cells) > 4 else ""
                client_care_phone = cells[5].get_text(strip=True) if len(cells) > 5 else ""
                initial_license_date = cells[6].get_text(strip=True) if len(cells) > 6 else ""
                
                # Parse and clean the data
                license_types = self.parse_license_types(licenses)
                products = self.parse_authorized_products(authorized_products)
                phone = self.clean_phone_number(client_care_phone)
                issue_date = self.parse_date(initial_license_date)
                license_status = self.determine_license_status(licenses)
                
                # Create document according to your MongoDB schema
                document = {
                    'business_name': license_holder,
                    'license_number': None,  # Not provided in this dataset
                    'stateName': 'Federal',
                    'city': None,  # Not provided in this dataset
                    'business_address': None,  # Not provided in this dataset
                    'contact_information': {
                        'phone': phone,
                        'email': None,
                        'website': None
                    },
                    'owner': {
                        'name': None,
                        'email': None,
                        'role': None,
                        'phone': phone,
                        'govt_issued_id': None
                    },
                    'operator_name': license_holder,
                    'issue_date': issue_date,
                    'expiration_date': None,
                    'license_type': license_types[0] if license_types else 'other',
                    'license_status': license_status,
                    'jurisdiction': province,
                    'regulatory_body': 'Health Canada',
                    'entity_type': license_types,
                    'filing_documents_url': None,
                    'license_conditions': [],
                    'claimed': False,
                    'claimedBy': None,
                    'claimedAt': None,
                    'canojaVerified': True,  # This is from official government source
                    'adminVerificationRequired': False,
                    'featured': False,
                    'dba': license_holder,
                    'state_license_document': None,
                    'utility_bill': None,
                    'gps_validation': False,
                    'location': {
                        'type': 'Point',
                        'coordinates': []
                    },
                    'smoke_shop': False,
                    # Additional fields specific to Health Canada data
                    'authorized_products': products,
                    'registered_patients_authorized': registered_patients,
                    'source': 'Health Canada Official Registry'
                }
                
                scraped_data.append(document)
                
            except Exception as e:
                print(f"Error processing row {i}: {e}")
                continue
        
        print(f"✓ Successfully scraped {len(scraped_data)} records")
        return scraped_data
    
    def save_to_json(self, data: List[Dict[str, Any]], filename: str = 'federal.json'):
        """Save scraped data to JSON file"""
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False, default=str)
            print(f"✓ Data saved to {filename}")
        except Exception as e:
            print(f"Error saving to JSON: {e}")
    
    def save_to_excel(self, data: List[Dict[str, Any]], filename: str = 'health_canada_cannabis_licenses.xlsx'):
        """Save scraped data to Excel file"""
        try:
            # Flatten the nested dictionaries for Excel export
            flattened_data = []
            for record in data:
                flat_record = {}
                for key, value in record.items():
                    if isinstance(value, dict):
                        for sub_key, sub_value in value.items():
                            flat_record[f"{key}_{sub_key}"] = sub_value
                    elif isinstance(value, list):
                        flat_record[key] = ', '.join(map(str, value)) if value else ''
                    else:
                        flat_record[key] = value
                flattened_data.append(flat_record)
            
            df = pd.DataFrame(flattened_data)
            df.to_excel(filename, index=False)
            print(f"✓ Data saved to {filename}")
        except Exception as e:
            print(f"Error saving to Excel: {e}")
    
    def print_sample_data(self, data: List[Dict[str, Any]], num_samples: int = 3):
        """Print sample data for verification"""
        print(f"\n=== Sample of {min(num_samples, len(data))} scraped records ===")
        for i, record in enumerate(data[:num_samples]):
            print(f"\nRecord {i + 1}:")
            print(f"Business Name: {record['business_name']}")
            print(f"Province: {record['stateName']}")
            print(f"License Type: {record['license_type']}")
            print(f"License Status: {record['license_status']}")
            print(f"Issue Date: {record['issue_date']}")
            print(f"Phone: {record['contact_information']['phone']}")
            print(f"Authorized Products: {record['authorized_products']}")
            print(f"Registered Patients: {record['registered_patients_authorized']}")
            print("-" * 50)


# MongoDB insertion function
def insert_to_mongodb(documents: List[Dict[str, Any]], connection_string: str, database_name: str):
    """Insert scraped data into MongoDB"""
    try:
        from pymongo import MongoClient
        
        client = MongoClient(connection_string)
        db = client[database_name]
        collection = db['testrecords']
        
        # Insert documents
        result = collection.insert_many(documents)
        print(f"✓ Inserted {len(result.inserted_ids)} documents into MongoDB")
        
        client.close()
    except ImportError:
        print("PyMongo not installed. Run: pip install pymongo")
    except Exception as e:
        print(f"Error inserting to MongoDB: {e}")


# Usage example
if __name__ == "__main__":
    print("=== Health Canada Cannabis License Scraper ===")
    
    # Initialize scraper
    scraper = HealthCanadaLicenseScraper()
    
    # Scrape data from the website
    scraped_data = scraper.scrape_table_data()
    
    if scraped_data:
        # Print sample data
        scraper.print_sample_data(scraped_data, 3)
        
        # Save to different formats
        scraper.save_to_json(scraped_data)
        scraper.save_to_excel(scraped_data)
        
        print(f"\n✓ Total records processed: {len(scraped_data)}")
         
        try:
            insert_to_mongodb(
                scraped_data,
                'mongodb://localhost:27017/',
                'cannabis_licenses'
            )
        except Exception as e:
            print(f"MongoDB insertion failed: {e}")
            
    else:
        print("No data could be scraped from the website")
        print("This might be due to:")
        print("1. Website structure changes")
        print("2. Network connectivity issues") 
        print("3. The page requiring JavaScript to load content")
        
    print("\n=== Scraping completed ===")