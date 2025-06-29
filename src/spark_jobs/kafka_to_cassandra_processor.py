"""
Spark Streaming Job: Kafka to Cassandra Processor
Consumes data from Kafka topics and processes it into Cassandra tables
"""

import json
import logging
from typing import Dict, Any
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, current_timestamp, 
    when, regexp_replace, split, date_format, lit
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DecimalType, TimestampType, BooleanType
)
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class KafkaToCassandraProcessor:
    """
    Spark Streaming processor for Kafka to Cassandra data pipeline
    """
    
    def __init__(self):
        """
        Initialize the Spark streaming processor
        """
        self.spark = self._create_spark_session()
        self.kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
        self.cassandra_hosts = os.getenv('CASSANDRA_HOSTS', 'cassandra')
        self.cassandra_keyspace = os.getenv('CASSANDRA_KEYSPACE', 'datawarehouse')
        
        # Kafka topics
        self.customer_topic = os.getenv('KAFKA_TOPIC_CUSTOMERS', 'customers')
        self.purchase_topic = os.getenv('KAFKA_TOPIC_PURCHASES', 'purchases')
        
        # Define schemas for the data
        self.customer_schema = self._get_customer_schema()
        self.purchase_schema = self._get_purchase_schema()
        
        logger.info("Kafka to Cassandra processor initialized")
    
    def _create_spark_session(self) -> SparkSession:
        """
        Create and configure Spark session
        
        Returns:
            Configured SparkSession
        """
        try:
            spark = SparkSession.builder \
                .appName("KafkaToCassandraProcessor") \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .config("spark.cassandra.connection.host", os.getenv('CASSANDRA_HOSTS', 'cassandra')) \
                .config("spark.cassandra.connection.port", "9042") \
                .config("spark.cassandra.auth.username", "") \
                .config("spark.cassandra.auth.password", "") \
                .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoint") \
                .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
                .getOrCreate()
            
            spark.sparkContext.setLogLevel("WARN")
            logger.info("Spark session created successfully")
            return spark
            
        except Exception as e:
            logger.error(f"Failed to create Spark session: {e}")
            raise
    
    def _get_customer_schema(self) -> StructType:
        """
        Define schema for customer data
        
        Returns:
            StructType schema for customer data
        """
        return StructType([
            StructField("event_type", StringType(), True),
            StructField("timestamp", StringType(), True),
            StructField("data", StructType([
                StructField("customer_id", StringType(), True),
                StructField("first_name", StringType(), True),
                StructField("last_name", StringType(), True),
                StructField("email", StringType(), True),
                StructField("phone", StringType(), True),
                StructField("address", StringType(), True),
                StructField("city", StringType(), True),
                StructField("state", StringType(), True),
                StructField("country", StringType(), True),
                StructField("zip_code", StringType(), True),
                StructField("date_of_birth", StringType(), True),
                StructField("gender", StringType(), True),
                StructField("created_at", StringType(), True),
                StructField("updated_at", StringType(), True)
            ]), True)
        ])
    
    def _get_purchase_schema(self) -> StructType:
        """
        Define schema for purchase data
        
        Returns:
            StructType schema for purchase data
        """
        return StructType([
            StructField("event_type", StringType(), True),
            StructField("timestamp", StringType(), True),
            StructField("data", StructType([
                StructField("purchase_id", StringType(), True),
                StructField("customer_id", StringType(), True),
                StructField("product_name", StringType(), True),
                StructField("product_category", StringType(), True),
                StructField("product_price", DecimalType(10, 2), True),
                StructField("quantity", IntegerType(), True),
                StructField("total_amount", DecimalType(10, 2), True),
                StructField("purchase_date", StringType(), True),
                StructField("payment_method", StringType(), True),
                StructField("store_location", StringType(), True),
                StructField("discount_amount", DecimalType(10, 2), True),
                StructField("tax_amount", DecimalType(10, 2), True),
                StructField("created_at", StringType(), True)
            ]), True)
        ])
    
    def read_kafka_stream(self, topic: str):
        """
        Read streaming data from Kafka topic
        
        Args:
            topic: Kafka topic name
            
        Returns:
            Streaming DataFrame
        """
        try:
            df = self.spark \
                .readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", self.kafka_servers) \
                .option("subscribe", topic) \
                .option("startingOffsets", "latest") \
                .option("failOnDataLoss", "false") \
                .load()
            
            logger.info(f"Created Kafka stream for topic: {topic}")
            return df
            
        except Exception as e:
            logger.error(f"Failed to create Kafka stream for topic {topic}: {e}")
            raise
    
    def process_customer_stream(self):
        """
        Process customer data stream from Kafka to Cassandra
        """
        logger.info("Starting customer stream processing...")
        
        # Read from Kafka
        kafka_df = self.read_kafka_stream(self.customer_topic)
        
        # Parse JSON data
        parsed_df = kafka_df.select(
            col("key").cast("string").alias("kafka_key"),
            from_json(col("value").cast("string"), self.customer_schema).alias("parsed_data"),
            col("timestamp").alias("kafka_timestamp")
        )
        
        # Extract customer data
        customer_df = parsed_df.select(
            col("parsed_data.data.customer_id").alias("customer_id"),
            col("parsed_data.data.first_name").alias("first_name"),
            col("parsed_data.data.last_name").alias("last_name"),
            col("parsed_data.data.email").alias("email"),
            col("parsed_data.data.phone").alias("phone"),
            col("parsed_data.data.address").alias("address"),
            col("parsed_data.data.city").alias("city"),
            col("parsed_data.data.state").alias("state"),
            col("parsed_data.data.country").alias("country"),
            col("parsed_data.data.zip_code").alias("zip_code"),
            to_timestamp(col("parsed_data.data.date_of_birth")).alias("date_of_birth"),
            col("parsed_data.data.gender").alias("gender"),
            to_timestamp(col("parsed_data.data.created_at")).alias("created_at"),
            to_timestamp(col("parsed_data.data.updated_at")).alias("updated_at")
        ).filter(col("customer_id").isNotNull())
        
        # Write to Cassandra
        query = customer_df.writeStream \
            .format("org.apache.spark.sql.cassandra") \
            .option("keyspace", self.cassandra_keyspace) \
            .option("table", "customers") \
            .option("checkpointLocation", "/tmp/spark-checkpoint/customers") \
            .outputMode("append") \
            .start()
        
        logger.info("Customer stream processing started")
        return query
    
    def process_purchase_stream(self):
        """
        Process purchase data stream from Kafka to Cassandra
        """
        logger.info("Starting purchase stream processing...")
        
        # Read from Kafka
        kafka_df = self.read_kafka_stream(self.purchase_topic)
        
        # Parse JSON data
        parsed_df = kafka_df.select(
            col("key").cast("string").alias("kafka_key"),
            from_json(col("value").cast("string"), self.purchase_schema).alias("parsed_data"),
            col("timestamp").alias("kafka_timestamp")
        )
        
        # Extract purchase data
        purchase_df = parsed_df.select(
            col("parsed_data.data.customer_id").alias("customer_id"),
            col("parsed_data.data.purchase_id").alias("purchase_id"),
            col("parsed_data.data.product_name").alias("product_name"),
            col("parsed_data.data.product_category").alias("product_category"),
            col("parsed_data.data.product_price").alias("product_price"),
            col("parsed_data.data.quantity").alias("quantity"),
            col("parsed_data.data.total_amount").alias("total_amount"),
            to_timestamp(col("parsed_data.data.purchase_date")).alias("purchase_date"),
            col("parsed_data.data.payment_method").alias("payment_method"),
            col("parsed_data.data.store_location").alias("store_location"),
            col("parsed_data.data.discount_amount").alias("discount_amount"),
            col("parsed_data.data.tax_amount").alias("tax_amount"),
            to_timestamp(col("parsed_data.data.created_at")).alias("created_at")
        ).filter(col("purchase_id").isNotNull())
        
        # Write to main purchases table
        main_query = purchase_df.writeStream \
            .format("org.apache.spark.sql.cassandra") \
            .option("keyspace", self.cassandra_keyspace) \
            .option("table", "purchases") \
            .option("checkpointLocation", "/tmp/spark-checkpoint/purchases") \
            .outputMode("append") \
            .start()
        
        # Also write to time-partitioned table for analytics
        time_partitioned_df = purchase_df.select(
            date_format(col("purchase_date"), "yyyy-MM-dd").alias("date_bucket"),
            col("purchase_date").alias("purchase_time"),
            col("purchase_id"),
            col("customer_id"),
            col("product_name"),
            col("product_category"),
            col("product_price"),
            col("quantity"),
            col("total_amount"),
            col("payment_method"),
            col("store_location")
        )
        
        time_query = time_partitioned_df.writeStream \
            .format("org.apache.spark.sql.cassandra") \
            .option("keyspace", self.cassandra_keyspace) \
            .option("table", "purchases_by_date") \
            .option("checkpointLocation", "/tmp/spark-checkpoint/purchases_by_date") \
            .outputMode("append") \
            .start()
        
        # Write to category-partitioned table
        category_partitioned_df = purchase_df.select(
            col("product_category"),
            col("purchase_date"),
            col("purchase_id"),
            col("customer_id"),
            col("product_name"),
            col("product_price"),
            col("quantity"),
            col("total_amount"),
            col("payment_method"),
            col("store_location")
        )
        
        category_query = category_partitioned_df.writeStream \
            .format("org.apache.spark.sql.cassandra") \
            .option("keyspace", self.cassandra_keyspace) \
            .option("table", "purchases_by_category") \
            .option("checkpointLocation", "/tmp/spark-checkpoint/purchases_by_category") \
            .outputMode("append") \
            .start()
        
        logger.info("Purchase stream processing started")
        return [main_query, time_query, category_query]
    
    def start_processing(self):
        """
        Start all streaming processes
        """
        logger.info("Starting all streaming processes...")
        
        try:
            # Start customer processing
            customer_query = self.process_customer_stream()
            
            # Start purchase processing
            purchase_queries = self.process_purchase_stream()
            
            # Collect all queries
            all_queries = [customer_query] + purchase_queries
            
            logger.info(f"Started {len(all_queries)} streaming queries")
            
            # Wait for all queries to finish
            for query in all_queries:
                query.awaitTermination()
                
        except Exception as e:
            logger.error(f"Error in streaming processing: {e}")
            raise
        finally:
            self.spark.stop()
    
    def stop_processing(self):
        """
        Stop all streaming processes
        """
        logger.info("Stopping streaming processes...")
        
        # Stop all active streams
        for stream in self.spark.streams.active:
            stream.stop()
        
        self.spark.stop()
        logger.info("All streaming processes stopped")


def main():
    """
    Main function to run the Kafka to Cassandra processor
    """
    processor = None
    
    try:
        # Create processor
        processor = KafkaToCassandraProcessor()
        
        # Start processing
        logger.info("Starting Kafka to Cassandra data processing...")
        processor.start_processing()
        
    except KeyboardInterrupt:
        logger.info("Received interrupt signal")
    except Exception as e:
        logger.error(f"Error in main: {e}")
        raise
    finally:
        if processor:
            processor.stop_processing()


if __name__ == "__main__":
    main() 