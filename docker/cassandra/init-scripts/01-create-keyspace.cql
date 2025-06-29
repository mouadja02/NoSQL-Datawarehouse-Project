/*
 * NoSQL Data Warehouse - Cassandra Initialization Script
 * This script creates the keyspace and tables for our data warehouse
 */

-- Create the main keyspace for our data warehouse
CREATE KEYSPACE IF NOT EXISTS datawarehouse
WITH REPLICATION = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
};

-- Use the keyspace
USE datawarehouse;

-- Create customers table
-- This table stores customer information with customer_id as partition key
CREATE TABLE IF NOT EXISTS customers (
    customer_id UUID PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    email TEXT,
    phone TEXT,
    address TEXT,
    city TEXT,
    state TEXT,
    country TEXT,
    zip_code TEXT,
    date_of_birth DATE,
    gender TEXT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);

-- Create purchases table
-- This table stores purchase transactions with customer_id as partition key
-- and purchase_id as clustering key for efficient querying
CREATE TABLE IF NOT EXISTS purchases (
    customer_id UUID,
    purchase_id UUID,
    product_name TEXT,
    product_category TEXT,
    product_price DECIMAL,
    quantity INT,
    total_amount DECIMAL,
    purchase_date TIMESTAMP,
    payment_method TEXT,
    store_location TEXT,
    discount_amount DECIMAL,
    tax_amount DECIMAL,
    created_at TIMESTAMP,
    PRIMARY KEY (customer_id, purchase_id)
) WITH CLUSTERING ORDER BY (purchase_id DESC);

-- Create purchases_by_date table for time-based queries
-- This table allows efficient querying by date ranges
CREATE TABLE IF NOT EXISTS purchases_by_date (
    date_bucket TEXT,  -- Format: YYYY-MM-DD
    purchase_time TIMESTAMP,
    purchase_id UUID,
    customer_id UUID,
    product_name TEXT,
    product_category TEXT,
    product_price DECIMAL,
    quantity INT,
    total_amount DECIMAL,
    payment_method TEXT,
    store_location TEXT,
    PRIMARY KEY (date_bucket, purchase_time, purchase_id)
) WITH CLUSTERING ORDER BY (purchase_time DESC, purchase_id ASC);

-- Create purchases_by_category table for category-based analytics
-- This table enables efficient querying by product category
CREATE TABLE IF NOT EXISTS purchases_by_category (
    product_category TEXT,
    purchase_date TIMESTAMP,
    purchase_id UUID,
    customer_id UUID,
    product_name TEXT,
    product_price DECIMAL,
    quantity INT,
    total_amount DECIMAL,
    payment_method TEXT,
    store_location TEXT,
    PRIMARY KEY (product_category, purchase_date, purchase_id)
) WITH CLUSTERING ORDER BY (purchase_date DESC, purchase_id ASC);

-- Create customer_summary table for aggregated customer metrics
-- This table stores pre-computed customer statistics
CREATE TABLE IF NOT EXISTS customer_summary (
    customer_id UUID PRIMARY KEY,
    total_purchases INT,
    total_spent DECIMAL,
    average_order_value DECIMAL,
    first_purchase_date TIMESTAMP,
    last_purchase_date TIMESTAMP,
    favorite_category TEXT,
    preferred_payment_method TEXT,
    last_updated TIMESTAMP
);

-- Create real-time streaming table for current processing
-- This table holds data being processed in real-time
CREATE TABLE IF NOT EXISTS streaming_events (
    event_id UUID,
    event_type TEXT,
    event_timestamp TIMESTAMP,
    customer_id UUID,
    event_data TEXT,  -- JSON string
    processed BOOLEAN,
    created_at TIMESTAMP,
    PRIMARY KEY (event_type, event_timestamp, event_id)
) WITH CLUSTERING ORDER BY (event_timestamp DESC, event_id ASC);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS ON customers (email);
CREATE INDEX IF NOT EXISTS ON customers (city);
CREATE INDEX IF NOT EXISTS ON purchases (product_category);
CREATE INDEX IF NOT EXISTS ON purchases (purchase_date);

-- Insert sample data for testing (optional)
-- This can be uncommented for initial testing
/*
INSERT INTO customers (customer_id, first_name, last_name, email, phone, address, city, state, country, zip_code, date_of_birth, gender, created_at, updated_at)
VALUES (uuid(), 'John', 'Doe', 'john.doe@example.com', '+1-555-0123', '123 Main St', 'New York', 'NY', 'USA', '10001', '1990-01-15', 'Male', toTimestamp(now()), toTimestamp(now()));

INSERT INTO customers (customer_id, first_name, last_name, email, phone, address, city, state, country, zip_code, date_of_birth, gender, created_at, updated_at)
VALUES (uuid(), 'Jane', 'Smith', 'jane.smith@example.com', '+1-555-0124', '456 Oak Ave', 'Los Angeles', 'CA', 'USA', '90210', '1985-03-22', 'Female', toTimestamp(now()), toTimestamp(now()));
*/ 