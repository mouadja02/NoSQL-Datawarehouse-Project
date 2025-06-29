"""
Kafka Data Stream Producer
Streams generated customer and purchase data to Kafka topics for real-time processing
"""

import json
import time
import logging
import signal
import sys
from typing import Dict, List, Optional
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError
import os
from pathlib import Path

# Add the parent directory to the path to import data generators
sys.path.append(str(Path(__file__).parent.parent))
from data_generation.customer_generator import CustomerGenerator
from data_generation.purchase_generator import PurchaseGenerator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DataStreamProducer:
    """
    Kafka producer for streaming customer and purchase data
    """
    
    def __init__(self, 
                 bootstrap_servers: str = 'localhost:9092',
                 customer_topic: str = 'customers',
                 purchase_topic: str = 'purchases'):
        """
        Initialize the Kafka producer
        
        Args:
            bootstrap_servers: Kafka bootstrap servers
            customer_topic: Topic for customer data
            purchase_topic: Topic for purchase data
        """
        self.bootstrap_servers = bootstrap_servers
        self.customer_topic = customer_topic
        self.purchase_topic = purchase_topic
        
        # Initialize Kafka producer
        self.producer = self._create_producer()
        
        # Initialize data generators
        self.customer_generator = CustomerGenerator()
        self.purchase_generator = PurchaseGenerator()
        
        # Track customer IDs for realistic purchase generation
        self.customer_ids = []
        
        # Control flags
        self.running = True
        self.stats = {
            'customers_sent': 0,
            'purchases_sent': 0,
            'errors': 0,
            'start_time': None
        }
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _create_producer(self) -> KafkaProducer:
        """
        Create and configure Kafka producer
        
        Returns:
            Configured KafkaProducer instance
        """
        try:
            producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
                key_serializer=lambda k: str(k).encode('utf-8') if k else None,
                # Producer configuration for reliability
                acks='all',  # Wait for all replicas to acknowledge
                retries=3,   # Retry failed sends
                batch_size=16384,  # Batch size in bytes
                linger_ms=10,      # Wait up to 10ms to batch messages
                buffer_memory=33554432,  # Total memory for buffering
                # Compression
                compression_type='gzip',
                # Error handling
                request_timeout_ms=30000,
                retry_backoff_ms=100
            )
            
            logger.info(f"Kafka producer created successfully. Bootstrap servers: {self.bootstrap_servers}")
            return producer
            
        except Exception as e:
            logger.error(f"Failed to create Kafka producer: {e}")
            raise
    
    def _signal_handler(self, signum, frame):
        """
        Handle shutdown signals gracefully
        """
        logger.info(f"Received signal {signum}. Shutting down gracefully...")
        self.running = False
    
    def send_customer(self, customer: Dict) -> bool:
        """
        Send customer data to Kafka topic
        
        Args:
            customer: Customer data dictionary
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Use customer_id as the key for partitioning
            key = customer['customer_id']
            
            # Add metadata
            message = {
                'event_type': 'customer_created',
                'timestamp': datetime.now().isoformat(),
                'data': customer
            }
            
            # Send to Kafka
            future = self.producer.send(
                topic=self.customer_topic,
                key=key,
                value=message
            )
            
            # Wait for the message to be sent (with timeout)
            record_metadata = future.get(timeout=10)
            
            logger.debug(f"Customer sent to {record_metadata.topic} "
                        f"partition {record_metadata.partition} "
                        f"offset {record_metadata.offset}")
            
            self.stats['customers_sent'] += 1
            
            # Store customer ID for purchase generation
            if key not in self.customer_ids:
                self.customer_ids.append(key)
            
            return True
            
        except KafkaError as e:
            logger.error(f"Failed to send customer to Kafka: {e}")
            self.stats['errors'] += 1
            return False
        except Exception as e:
            logger.error(f"Unexpected error sending customer: {e}")
            self.stats['errors'] += 1
            return False
    
    def send_purchase(self, purchase: Dict) -> bool:
        """
        Send purchase data to Kafka topic
        
        Args:
            purchase: Purchase data dictionary
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Use customer_id as the key for partitioning
            key = purchase['customer_id']
            
            # Add metadata
            message = {
                'event_type': 'purchase_made',
                'timestamp': datetime.now().isoformat(),
                'data': purchase
            }
            
            # Send to Kafka
            future = self.producer.send(
                topic=self.purchase_topic,
                key=key,
                value=message
            )
            
            # Wait for the message to be sent (with timeout)
            record_metadata = future.get(timeout=10)
            
            logger.debug(f"Purchase sent to {record_metadata.topic} "
                        f"partition {record_metadata.partition} "
                        f"offset {record_metadata.offset}")
            
            self.stats['purchases_sent'] += 1
            return True
            
        except KafkaError as e:
            logger.error(f"Failed to send purchase to Kafka: {e}")
            self.stats['errors'] += 1
            return False
        except Exception as e:
            logger.error(f"Unexpected error sending purchase: {e}")
            self.stats['errors'] += 1
            return False
    
    def generate_and_stream_customers(self, 
                                    count: int, 
                                    delay_seconds: float = 1.0) -> None:
        """
        Generate and stream customer data
        
        Args:
            count: Number of customers to generate
            delay_seconds: Delay between messages
        """
        logger.info(f"Starting to stream {count} customers with {delay_seconds}s delay")
        
        for i in range(count):
            if not self.running:
                break
                
            # Generate customer
            customer = self.customer_generator.generate_customer()
            
            # Send to Kafka
            if self.send_customer(customer):
                logger.info(f"Streamed customer {i+1}/{count}: {customer['email']}")
            
            # Progress logging
            if (i + 1) % 100 == 0:
                logger.info(f"Customers streamed: {i + 1}/{count}")
            
            # Delay before next message
            if delay_seconds > 0:
                time.sleep(delay_seconds)
        
        logger.info(f"Finished streaming {count} customers")
    
    def generate_and_stream_purchases(self, 
                                    count: int, 
                                    delay_seconds: float = 0.5,
                                    use_existing_customers: bool = True) -> None:
        """
        Generate and stream purchase data
        
        Args:
            count: Number of purchases to generate
            delay_seconds: Delay between messages
            use_existing_customers: Whether to use existing customer IDs
        """
        logger.info(f"Starting to stream {count} purchases with {delay_seconds}s delay")
        
        customer_ids_to_use = self.customer_ids if use_existing_customers and self.customer_ids else None
        
        for i in range(count):
            if not self.running:
                break
            
            # Generate purchase
            if customer_ids_to_use:
                purchase = self.purchase_generator.generate_purchase(
                    customer_id=self.customer_generator.fake.random.choice(customer_ids_to_use)
                )
            else:
                purchase = self.purchase_generator.generate_purchase()
            
            # Send to Kafka
            if self.send_purchase(purchase):
                logger.info(f"Streamed purchase {i+1}/{count}: {purchase['product_name']}")
            
            # Progress logging
            if (i + 1) % 100 == 0:
                logger.info(f"Purchases streamed: {i + 1}/{count}")
            
            # Delay before next message
            if delay_seconds > 0:
                time.sleep(delay_seconds)
        
        logger.info(f"Finished streaming {count} purchases")
    
    def stream_mixed_data(self, 
                         duration_minutes: int = 60,
                         customers_per_minute: int = 10,
                         purchases_per_minute: int = 50) -> None:
        """
        Stream mixed customer and purchase data for a specified duration
        
        Args:
            duration_minutes: How long to stream data
            customers_per_minute: Rate of customer generation
            purchases_per_minute: Rate of purchase generation
        """
        logger.info(f"Starting mixed data streaming for {duration_minutes} minutes")
        logger.info(f"Rate: {customers_per_minute} customers/min, {purchases_per_minute} purchases/min")
        
        self.stats['start_time'] = datetime.now()
        
        # Calculate delays
        customer_delay = 60.0 / customers_per_minute if customers_per_minute > 0 else float('inf')
        purchase_delay = 60.0 / purchases_per_minute if purchases_per_minute > 0 else float('inf')
        
        # Track timing
        start_time = time.time()
        end_time = start_time + (duration_minutes * 60)
        
        last_customer_time = start_time
        last_purchase_time = start_time
        
        while time.time() < end_time and self.running:
            current_time = time.time()
            
            # Send customer if it's time
            if current_time - last_customer_time >= customer_delay:
                customer = self.customer_generator.generate_customer()
                self.send_customer(customer)
                last_customer_time = current_time
            
            # Send purchase if it's time
            if current_time - last_purchase_time >= purchase_delay:
                customer_ids_to_use = self.customer_ids if self.customer_ids else None
                purchase = self.purchase_generator.generate_purchase(
                    customer_id=self.customer_generator.fake.random.choice(customer_ids_to_use) 
                    if customer_ids_to_use else None
                )
                self.send_purchase(purchase)
                last_purchase_time = current_time
            
            # Small sleep to prevent busy waiting
            time.sleep(0.1)
            
            # Log statistics every minute
            if int(current_time) % 60 == 0:
                self.log_statistics()
        
        logger.info("Finished mixed data streaming")
        self.log_statistics()
    
    def log_statistics(self) -> None:
        """
        Log current streaming statistics
        """
        if self.stats['start_time']:
            duration = datetime.now() - self.stats['start_time']
            duration_minutes = duration.total_seconds() / 60
            
            customer_rate = self.stats['customers_sent'] / duration_minutes if duration_minutes > 0 else 0
            purchase_rate = self.stats['purchases_sent'] / duration_minutes if duration_minutes > 0 else 0
            
            logger.info(f"Statistics - Duration: {duration_minutes:.1f}m, "
                       f"Customers: {self.stats['customers_sent']} ({customer_rate:.1f}/min), "
                       f"Purchases: {self.stats['purchases_sent']} ({purchase_rate:.1f}/min), "
                       f"Errors: {self.stats['errors']}")
    
    def create_topics(self) -> None:
        """
        Create Kafka topics if they don't exist
        """
        from kafka.admin import KafkaAdminClient, NewTopic
        
        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers=self.bootstrap_servers,
                client_id='data_stream_producer_admin'
            )
            
            # Define topics
            topics = [
                NewTopic(
                    name=self.customer_topic,
                    num_partitions=3,
                    replication_factor=1
                ),
                NewTopic(
                    name=self.purchase_topic,
                    num_partitions=3,
                    replication_factor=1
                )
            ]
            
            # Create topics
            admin_client.create_topics(topics, validate_only=False)
            logger.info(f"Created topics: {self.customer_topic}, {self.purchase_topic}")
            
        except Exception as e:
            logger.warning(f"Could not create topics (they may already exist): {e}")
    
    def close(self) -> None:
        """
        Close the producer and clean up resources
        """
        try:
            if self.producer:
                self.producer.flush()  # Ensure all messages are sent
                self.producer.close()
                logger.info("Kafka producer closed successfully")
        except Exception as e:
            logger.error(f"Error closing producer: {e}")


def main():
    """
    Main function for running the data stream producer
    """
    # Configuration from environment variables
    bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    customer_topic = os.getenv('KAFKA_TOPIC_CUSTOMERS', 'customers')
    purchase_topic = os.getenv('KAFKA_TOPIC_PURCHASES', 'purchases')
    
    # Create producer
    producer = DataStreamProducer(
        bootstrap_servers=bootstrap_servers,
        customer_topic=customer_topic,
        purchase_topic=purchase_topic
    )
    
    try:
        # Create topics
        producer.create_topics()
        
        # Stream data for demonstration
        logger.info("Starting data streaming demonstration...")
        
        # First, generate some customers
        producer.generate_and_stream_customers(count=50, delay_seconds=0.5)
        
        # Then generate purchases using those customers
        producer.generate_and_stream_purchases(count=200, delay_seconds=0.2)
        
        # Finally, stream mixed data for 5 minutes
        producer.stream_mixed_data(
            duration_minutes=5,
            customers_per_minute=20,
            purchases_per_minute=100
        )
        
    except KeyboardInterrupt:
        logger.info("Received interrupt signal")
    except Exception as e:
        logger.error(f"Error in main: {e}")
    finally:
        producer.close()
        logger.info("Data stream producer finished")


if __name__ == "__main__":
    main() 