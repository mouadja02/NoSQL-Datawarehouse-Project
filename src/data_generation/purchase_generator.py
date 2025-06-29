"""
Purchase Data Generator using Faker
Generates realistic purchase/transaction data for the NoSQL Data Warehouse project
"""

import json
import uuid
import random
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from faker import Faker
from faker.providers import internet, commerce, date_time
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PurchaseGenerator:
    """
    Generates realistic purchase/transaction data using Faker library
    """
    
    def __init__(self, locale: str = 'en_US', seed: Optional[int] = None):
        """
        Initialize the purchase generator
        
        Args:
            locale: Locale for generating data (default: en_US)
            seed: Random seed for reproducible data generation
        """
        self.fake = Faker(locale)
        if seed:
            Faker.seed(seed)
            random.seed(seed)
        
        # Add providers
        self.fake.add_provider(commerce)
        self.fake.add_provider(date_time)
        
        # Define product categories and their typical price ranges
        self.product_categories = {
            'Electronics': {'min_price': 50, 'max_price': 2000, 'avg_quantity': 1.2},
            'Clothing': {'min_price': 15, 'max_price': 300, 'avg_quantity': 2.1},
            'Home & Garden': {'min_price': 10, 'max_price': 500, 'avg_quantity': 1.8},
            'Books': {'min_price': 5, 'max_price': 50, 'avg_quantity': 1.5},
            'Sports & Outdoors': {'min_price': 20, 'max_price': 800, 'avg_quantity': 1.3},
            'Health & Beauty': {'min_price': 8, 'max_price': 150, 'avg_quantity': 2.5},
            'Toys & Games': {'min_price': 10, 'max_price': 200, 'avg_quantity': 1.7},
            'Food & Beverages': {'min_price': 5, 'max_price': 100, 'avg_quantity': 3.2},
            'Automotive': {'min_price': 25, 'max_price': 1000, 'avg_quantity': 1.1},
            'Jewelry': {'min_price': 30, 'max_price': 5000, 'avg_quantity': 1.0}
        }
        
        # Payment methods with their usage distribution
        self.payment_methods = ['Credit Card', 'Debit Card', 'PayPal', 'Apple Pay', 'Google Pay', 'Cash', 'Bank Transfer']
        self.payment_weights = [0.35, 0.25, 0.15, 0.08, 0.07, 0.05, 0.05]
        
        # Store locations
        self.store_locations = [
            'New York, NY', 'Los Angeles, CA', 'Chicago, IL', 'Houston, TX',
            'Phoenix, AZ', 'Philadelphia, PA', 'San Antonio, TX', 'San Diego, CA',
            'Dallas, TX', 'San Jose, CA', 'Austin, TX', 'Jacksonville, FL',
            'Fort Worth, TX', 'Columbus, OH', 'Charlotte, NC', 'San Francisco, CA',
            'Indianapolis, IN', 'Seattle, WA', 'Denver, CO', 'Washington, DC',
            'Boston, MA', 'El Paso, TX', 'Nashville, TN', 'Detroit, MI',
            'Oklahoma City, OK', 'Portland, OR', 'Las Vegas, NV', 'Memphis, TN',
            'Louisville, KY', 'Baltimore, MD', 'Milwaukee, WI', 'Albuquerque, NM',
            'Tucson, AZ', 'Fresno, CA', 'Sacramento, CA', 'Mesa, AZ',
            'Kansas City, MO', 'Atlanta, GA', 'Long Beach, CA', 'Colorado Springs, CO',
            'Raleigh, NC', 'Miami, FL', 'Virginia Beach, VA', 'Omaha, NE',
            'Oakland, CA', 'Minneapolis, MN', 'Tulsa, OK', 'Arlington, TX',
            'Tampa, FL', 'New Orleans, LA'
        ]
        
        # Seasonal factors for different categories
        self.seasonal_factors = {
            'Electronics': {1: 0.8, 2: 0.7, 3: 0.9, 4: 0.9, 5: 1.0, 6: 1.0, 
                          7: 1.1, 8: 1.2, 9: 1.1, 10: 1.0, 11: 1.3, 12: 1.5},
            'Clothing': {1: 0.7, 2: 0.8, 3: 1.1, 4: 1.2, 5: 1.1, 6: 1.0,
                        7: 0.9, 8: 1.0, 9: 1.2, 10: 1.1, 11: 1.3, 12: 1.4},
            'Home & Garden': {1: 0.6, 2: 0.7, 3: 1.2, 4: 1.4, 5: 1.3, 6: 1.2,
                             7: 1.1, 8: 1.0, 9: 0.9, 10: 0.8, 11: 0.7, 12: 0.8},
            'default': {1: 1.0, 2: 1.0, 3: 1.0, 4: 1.0, 5: 1.0, 6: 1.0,
                       7: 1.0, 8: 1.0, 9: 1.0, 10: 1.0, 11: 1.0, 12: 1.0}
        }
    
    def generate_purchase(self, customer_id: Optional[str] = None, 
                         purchase_date: Optional[datetime] = None) -> Dict:
        """
        Generate a single purchase record
        
        Args:
            customer_id: Customer ID (if None, generates random UUID)
            purchase_date: Purchase date (if None, generates random recent date)
            
        Returns:
            Dictionary containing purchase data
        """
        # Use provided customer_id or generate new one
        if customer_id is None:
            customer_id = str(uuid.uuid4())
        
        # Use provided date or generate random recent date
        if purchase_date is None:
            purchase_date = self.fake.date_time_between(
                start_date='-1y',
                end_date='now'
            )
        
        # Select product category based on seasonal factors
        month = purchase_date.month
        category = self._select_category_by_season(month)
        
        # Generate product details
        product_name = self._generate_product_name(category)
        product_price = self._generate_product_price(category)
        quantity = self._generate_quantity(category)
        
        # Calculate amounts
        subtotal = product_price * quantity
        discount_amount = self._generate_discount(subtotal)
        tax_amount = (subtotal - discount_amount) * 0.08  # 8% tax rate
        total_amount = subtotal - discount_amount + tax_amount
        
        # Select payment method and store location
        payment_method = random.choices(self.payment_methods, weights=self.payment_weights)[0]
        store_location = random.choice(self.store_locations)
        
        # Generate timestamps
        created_at = purchase_date + timedelta(minutes=random.randint(0, 60))
        
        purchase = {
            'purchase_id': str(uuid.uuid4()),
            'customer_id': customer_id,
            'product_name': product_name,
            'product_category': category,
            'product_price': round(product_price, 2),
            'quantity': quantity,
            'total_amount': round(total_amount, 2),
            'purchase_date': purchase_date.isoformat(),
            'payment_method': payment_method,
            'store_location': store_location,
            'discount_amount': round(discount_amount, 2),
            'tax_amount': round(tax_amount, 2),
            'created_at': created_at.isoformat()
        }
        
        return purchase
    
    def _select_category_by_season(self, month: int) -> str:
        """
        Select product category based on seasonal factors
        
        Args:
            month: Month number (1-12)
            
        Returns:
            Selected category
        """
        # Calculate weighted probabilities based on seasonal factors
        weighted_categories = []
        weights = []
        
        for category, info in self.product_categories.items():
            seasonal_factor = self.seasonal_factors.get(category, self.seasonal_factors['default'])
            weight = seasonal_factor.get(month, 1.0)
            weighted_categories.append(category)
            weights.append(weight)
        
        return random.choices(weighted_categories, weights=weights)[0]
    
    def _generate_product_name(self, category: str) -> str:
        """
        Generate a product name based on category
        
        Args:
            category: Product category
            
        Returns:
            Product name
        """
        # Category-specific product name patterns
        if category == 'Electronics':
            brands = ['Samsung', 'Apple', 'Sony', 'LG', 'HP', 'Dell', 'Canon', 'Nikon']
            products = ['Smartphone', 'Laptop', 'Tablet', 'Headphones', 'Camera', 'Monitor', 'Speaker', 'Keyboard']
            return f"{random.choice(brands)} {random.choice(products)} {random.choice(['Pro', 'Plus', 'Max', 'Ultra', ''])}"
        
        elif category == 'Clothing':
            brands = ['Nike', 'Adidas', 'Levi\'s', 'Gap', 'H&M', 'Zara', 'Uniqlo', 'Ralph Lauren']
            items = ['T-Shirt', 'Jeans', 'Sneakers', 'Jacket', 'Dress', 'Sweater', 'Pants', 'Shirt']
            return f"{random.choice(brands)} {random.choice(items)}"
        
        elif category == 'Books':
            return self.fake.catch_phrase() + " Book"
        
        elif category == 'Food & Beverages':
            foods = ['Organic Coffee', 'Green Tea', 'Dark Chocolate', 'Protein Bar', 'Energy Drink', 
                    'Nuts Mix', 'Fruit Juice', 'Yogurt', 'Cereal', 'Pasta']
            return random.choice(foods)
        
        else:
            # Generic product name using Faker
            return f"{self.fake.color_name()} {self.fake.word().title()}"
    
    def _generate_product_price(self, category: str) -> float:
        """
        Generate product price based on category
        
        Args:
            category: Product category
            
        Returns:
            Product price
        """
        price_info = self.product_categories.get(category, {'min_price': 10, 'max_price': 100})
        min_price = price_info['min_price']
        max_price = price_info['max_price']
        
        # Use log-normal distribution for more realistic pricing
        mean_price = (min_price + max_price) / 2
        std_dev = (max_price - min_price) / 4
        
        price = random.lognormvariate(
            mu=random.uniform(min_price, mean_price),
            sigma=std_dev / mean_price
        )
        
        # Ensure price is within bounds
        price = max(min_price, min(max_price, price))
        
        # Round to realistic price points
        if price < 10:
            return round(price, 2)
        elif price < 100:
            return round(price * 2) / 2  # Round to nearest 0.50
        else:
            return round(price)
    
    def _generate_quantity(self, category: str) -> int:
        """
        Generate purchase quantity based on category
        
        Args:
            category: Product category
            
        Returns:
            Quantity
        """
        avg_quantity = self.product_categories.get(category, {}).get('avg_quantity', 1.5)
        
        # Use Poisson distribution for quantity
        quantity = max(1, int(random.expovariate(1 / avg_quantity)))
        
        # Cap quantity at reasonable maximum
        return min(quantity, 10)
    
    def _generate_discount(self, subtotal: float) -> float:
        """
        Generate discount amount
        
        Args:
            subtotal: Subtotal amount
            
        Returns:
            Discount amount
        """
        # 30% chance of having a discount
        if random.random() < 0.3:
            # Discount between 5% and 25%
            discount_rate = random.uniform(0.05, 0.25)
            return subtotal * discount_rate
        return 0.0
    
    def generate_purchases_batch(self, count: int, customer_ids: Optional[List[str]] = None) -> List[Dict]:
        """
        Generate a batch of purchase records
        
        Args:
            count: Number of purchases to generate
            customer_ids: List of customer IDs to use (if None, generates random ones)
            
        Returns:
            List of purchase dictionaries
        """
        logger.info(f"Generating {count} purchase records...")
        
        purchases = []
        for i in range(count):
            if i % 1000 == 0 and i > 0:
                logger.info(f"Generated {i} purchases...")
            
            # Select customer ID if list provided
            customer_id = None
            if customer_ids:
                customer_id = random.choice(customer_ids)
            
            purchase = self.generate_purchase(customer_id=customer_id)
            purchases.append(purchase)
        
        logger.info(f"Successfully generated {count} purchase records")
        return purchases
    
    def generate_purchases_stream(self, count: int, customer_ids: Optional[List[str]] = None):
        """
        Generate purchases as a stream (generator)
        
        Args:
            count: Number of purchases to generate
            customer_ids: List of customer IDs to use
            
        Yields:
            Purchase dictionary
        """
        logger.info(f"Starting purchase stream generation for {count} records...")
        
        for i in range(count):
            if i % 1000 == 0 and i > 0:
                logger.info(f"Generated {i} purchases...")
            
            customer_id = None
            if customer_ids:
                customer_id = random.choice(customer_ids)
            
            yield self.generate_purchase(customer_id=customer_id)
    
    def save_to_json(self, purchases: List[Dict], filename: str):
        """
        Save purchases to JSON file
        
        Args:
            purchases: List of purchase dictionaries
            filename: Output filename
        """
        try:
            with open(filename, 'w') as f:
                json.dump(purchases, f, indent=2, default=str)
            logger.info(f"Saved {len(purchases)} purchases to {filename}")
        except Exception as e:
            logger.error(f"Error saving purchases to file: {e}")
            raise
    
    def get_purchase_statistics(self, purchases: List[Dict]) -> Dict:
        """
        Get statistics about generated purchases
        
        Args:
            purchases: List of purchase dictionaries
            
        Returns:
            Statistics dictionary
        """
        if not purchases:
            return {}
        
        # Calculate statistics
        total_revenue = sum(p['total_amount'] for p in purchases)
        avg_order_value = total_revenue / len(purchases)
        
        # Category distribution
        category_counts = {}
        category_revenue = {}
        payment_method_counts = {}
        
        for purchase in purchases:
            category = purchase['product_category']
            payment = purchase['payment_method']
            amount = purchase['total_amount']
            
            category_counts[category] = category_counts.get(category, 0) + 1
            category_revenue[category] = category_revenue.get(category, 0) + amount
            payment_method_counts[payment] = payment_method_counts.get(payment, 0) + 1
        
        # Top categories by revenue
        top_categories = sorted(category_revenue.items(), key=lambda x: x[1], reverse=True)
        
        statistics = {
            'total_purchases': len(purchases),
            'total_revenue': round(total_revenue, 2),
            'average_order_value': round(avg_order_value, 2),
            'category_distribution': category_counts,
            'category_revenue': {k: round(v, 2) for k, v in category_revenue.items()},
            'payment_method_distribution': payment_method_counts,
            'top_categories_by_revenue': [(cat, round(rev, 2)) for cat, rev in top_categories]
        }
        
        return statistics


def main():
    """
    Main function for testing the purchase generator
    """
    # Initialize generator
    generator = PurchaseGenerator(seed=42)
    
    # Generate sample purchases
    purchases = generator.generate_purchases_batch(100)
    
    # Print sample purchase
    print("Sample Purchase:")
    print(json.dumps(purchases[0], indent=2))
    
    # Print statistics
    stats = generator.get_purchase_statistics(purchases)
    print("\nPurchase Statistics:")
    print(json.dumps(stats, indent=2))
    
    # Save to file
    generator.save_to_json(purchases, 'sample_purchases.json')


if __name__ == "__main__":
    main() 