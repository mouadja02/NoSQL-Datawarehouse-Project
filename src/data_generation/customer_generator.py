"""
Customer Data Generator using Faker
Generates realistic customer data for the NoSQL Data Warehouse project
"""

import json
import uuid
import random
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from faker import Faker
from faker.providers import internet, person, address, phone_number, date_time
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CustomerGenerator:
    """
    Generates realistic customer data using Faker library
    """
    
    def __init__(self, locale: str = 'en_US', seed: Optional[int] = None):
        """
        Initialize the customer generator
        
        Args:
            locale: Locale for generating data (default: en_US)
            seed: Random seed for reproducible data generation
        """
        self.fake = Faker(locale)
        if seed:
            Faker.seed(seed)
            random.seed(seed)
        
        # Add providers
        self.fake.add_provider(internet)
        self.fake.add_provider(person)
        self.fake.add_provider(address)
        self.fake.add_provider(phone_number)
        self.fake.add_provider(date_time)
        
        # Define some realistic data distributions
        self.genders = ['Male', 'Female', 'Other', 'Prefer not to say']
        self.gender_weights = [0.48, 0.48, 0.02, 0.02]
        
        # Age distribution (18-80 years old)
        self.min_age = 18
        self.max_age = 80
        
    def generate_customer(self) -> Dict:
        """
        Generate a single customer record
        
        Returns:
            Dictionary containing customer data
        """
        # Generate basic personal information
        gender = random.choices(self.genders, weights=self.gender_weights)[0]
        
        # Generate name based on gender
        if gender == 'Male':
            first_name = self.fake.first_name_male()
        elif gender == 'Female':
            first_name = self.fake.first_name_female()
        else:
            first_name = self.fake.first_name()
            
        last_name = self.fake.last_name()
        
        # Generate date of birth (18-80 years old)
        birth_date = self.fake.date_of_birth(
            minimum_age=self.min_age,
            maximum_age=self.max_age
        )
        
        # Generate contact information
        email = self._generate_email(first_name, last_name)
        phone = self.fake.phone_number()
        
        # Generate address
        address = self.fake.street_address()
        city = self.fake.city()
        state = self.fake.state()
        country = 'USA'  # Assuming US-based customers
        zip_code = self.fake.zipcode()
        
        # Generate timestamps
        created_at = self.fake.date_time_between(
            start_date='-2y',
            end_date='now'
        )
        updated_at = self.fake.date_time_between(
            start_date=created_at,
            end_date='now'
        )
        
        customer = {
            'customer_id': str(uuid.uuid4()),
            'first_name': first_name,
            'last_name': last_name,
            'email': email,
            'phone': phone,
            'address': address,
            'city': city,
            'state': state,
            'country': country,
            'zip_code': zip_code,
            'date_of_birth': birth_date.isoformat(),
            'gender': gender,
            'created_at': created_at.isoformat(),
            'updated_at': updated_at.isoformat()
        }
        
        return customer
    
    def _generate_email(self, first_name: str, last_name: str) -> str:
        """
        Generate a realistic email address based on name
        
        Args:
            first_name: Customer's first name
            last_name: Customer's last name
            
        Returns:
            Email address string
        """
        # Email patterns
        patterns = [
            f"{first_name.lower()}.{last_name.lower()}",
            f"{first_name.lower()}{last_name.lower()}",
            f"{first_name.lower()}_{last_name.lower()}",
            f"{first_name[0].lower()}{last_name.lower()}",
            f"{first_name.lower()}{random.randint(1, 999)}",
            f"{first_name.lower()}.{last_name.lower()}{random.randint(1, 99)}"
        ]
        
        # Email domains
        domains = [
            'gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com',
            'aol.com', 'icloud.com', 'protonmail.com', 'company.com'
        ]
        
        username = random.choice(patterns)
        domain = random.choice(domains)
        
        return f"{username}@{domain}"
    
    def generate_customers_batch(self, count: int) -> List[Dict]:
        """
        Generate a batch of customer records
        
        Args:
            count: Number of customers to generate
            
        Returns:
            List of customer dictionaries
        """
        logger.info(f"Generating {count} customer records...")
        
        customers = []
        for i in range(count):
            if i % 1000 == 0 and i > 0:
                logger.info(f"Generated {i} customers...")
            
            customer = self.generate_customer()
            customers.append(customer)
        
        logger.info(f"Successfully generated {count} customer records")
        return customers
    
    def generate_customers_stream(self, count: int):
        """
        Generate customers as a stream (generator)
        
        Args:
            count: Number of customers to generate
            
        Yields:
            Customer dictionary
        """
        logger.info(f"Starting customer stream generation for {count} records...")
        
        for i in range(count):
            if i % 1000 == 0 and i > 0:
                logger.info(f"Generated {i} customers...")
            
            yield self.generate_customer()
    
    def save_to_json(self, customers: List[Dict], filename: str):
        """
        Save customers to JSON file
        
        Args:
            customers: List of customer dictionaries
            filename: Output filename
        """
        try:
            with open(filename, 'w') as f:
                json.dump(customers, f, indent=2, default=str)
            logger.info(f"Saved {len(customers)} customers to {filename}")
        except Exception as e:
            logger.error(f"Error saving customers to file: {e}")
            raise
    
    def get_customer_statistics(self, customers: List[Dict]) -> Dict:
        """
        Get statistics about generated customers
        
        Args:
            customers: List of customer dictionaries
            
        Returns:
            Statistics dictionary
        """
        if not customers:
            return {}
        
        # Gender distribution
        gender_counts = {}
        city_counts = {}
        state_counts = {}
        
        for customer in customers:
            gender = customer['gender']
            city = customer['city']
            state = customer['state']
            
            gender_counts[gender] = gender_counts.get(gender, 0) + 1
            city_counts[city] = city_counts.get(city, 0) + 1
            state_counts[state] = state_counts.get(state, 0) + 1
        
        # Get top cities and states
        top_cities = sorted(city_counts.items(), key=lambda x: x[1], reverse=True)[:10]
        top_states = sorted(state_counts.items(), key=lambda x: x[1], reverse=True)[:10]
        
        statistics = {
            'total_customers': len(customers),
            'gender_distribution': gender_counts,
            'top_cities': dict(top_cities),
            'top_states': dict(top_states),
            'unique_cities': len(city_counts),
            'unique_states': len(state_counts)
        }
        
        return statistics


def main():
    """
    Main function for testing the customer generator
    """
    # Initialize generator
    generator = CustomerGenerator(seed=42)
    
    # Generate sample customers
    customers = generator.generate_customers_batch(100)
    
    # Print sample customer
    print("Sample Customer:")
    print(json.dumps(customers[0], indent=2))
    
    # Print statistics
    stats = generator.get_customer_statistics(customers)
    print("\nCustomer Statistics:")
    print(json.dumps(stats, indent=2))
    
    # Save to file
    generator.save_to_json(customers, 'sample_customers.json')


if __name__ == "__main__":
    main() 