-- Drop existing schema
-- DROP SCHEMA IF EXISTS public CASCADE;
-- CREATE SCHEMA public;

-- Create ENUM types
DO $$ BEGIN
    CREATE TYPE order_status_type AS ENUM (
      'ACCEPTED',
      'REJECTED', 
      'REJECTED_CUSTOMER',
      'REJECTED_RESTAURANT',
      'CANCELLED_CUSTOMER', 
      'CANCELLED_RESTAURANT',
      'COMPLETED'
    );
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

DO $$ BEGIN
    CREATE TYPE delivery_type AS ENUM (
      'DELIVERY',
      'COLLECTION', 
      'PICKUP',
      'UNKNOWN'
    );
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

-- Platforms table
CREATE TABLE IF NOT EXISTS platforms (
  id SERIAL PRIMARY KEY,
  name VARCHAR(50) UNIQUE NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Restaurants table
CREATE TABLE IF NOT EXISTS restaurants (
  id SERIAL PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  platform_id INTEGER REFERENCES platforms(id),
  external_id VARCHAR(100),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  UNIQUE(platform_id, name)
);

-- Create partial unique constraint for external_id
-- Drop index first if it exists
DROP INDEX IF EXISTS idx_restaurants_platform_external_id;
CREATE UNIQUE INDEX idx_restaurants_platform_external_id 
ON restaurants(platform_id, external_id) 
WHERE external_id IS NOT NULL;

-- Enhanced Orders table with analytics fields
CREATE TABLE IF NOT EXISTS orders (
  id SERIAL PRIMARY KEY,
  platform_id INTEGER REFERENCES platforms(id) NOT NULL,
  platform_order_id VARCHAR(100) NOT NULL,
  restaurant_id INTEGER REFERENCES restaurants(id) NOT NULL,
  order_status order_status_type NOT NULL,
  delivery_type delivery_type DEFAULT 'UNKNOWN',
  order_value DECIMAL(10,2),
  basket_size INTEGER,
  discount_amount DECIMAL(10,2),
  order_datetime TIMESTAMP,
  restaurant_wait_time_minutes INTEGER, -- Time restaurant took to prepare?
  total_delivery_time_minutes INTEGER,  -- End-to-end delivery time..?
  courier_wait_time_minutes INTEGER,    -- Time courier waited at restaurant?
  prep_time_minutes INTEGER,            -- Original prep time estimate?
  currency_code VARCHAR(3) DEFAULT 'GBP',
  auto_accept_status VARCHAR(50),        -- DeliveryPlatform2 auto accept status
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  UNIQUE(platform_id, platform_order_id)
);

-- Create partitions for orders table
-- CREATE TABLE IF NOT EXISTS orders_2024 PARTITION OF orders 
-- FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
-- 
-- CREATE TABLE IF NOT EXISTS orders_2025 PARTITION OF orders 
-- FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');
-- 
-- CREATE TABLE IF NOT EXISTS orders_2026 PARTITION OF orders 
-- FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');
-- 
-- CREATE TABLE IF NOT EXISTS orders_default PARTITION OF orders DEFAULT;

-- Ratings table
CREATE TABLE IF NOT EXISTS ratings (
  id SERIAL PRIMARY KEY,
  restaurant_id INTEGER REFERENCES restaurants(id) NOT NULL,
  platform_id INTEGER REFERENCES platforms(id) NOT NULL,
  platform_order_id VARCHAR(100),
  rating_value DECIMAL(3,2) NOT NULL,
  rating_type VARCHAR(50) DEFAULT 'overall',
  comment TEXT,
  rating_date TIMESTAMP,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create partial unique constraint for ratings
DROP INDEX IF EXISTS idx_ratings_platform_order_type;
CREATE UNIQUE INDEX idx_ratings_platform_order_type 
ON ratings(platform_id, platform_order_id, rating_type) 
WHERE platform_order_id IS NOT NULL;

-- Integrations table with enhanced field mappings
CREATE TABLE IF NOT EXISTS integrations (
  id SERIAL PRIMARY KEY,
  name VARCHAR(100) UNIQUE NOT NULL,
  platform_id INTEGER REFERENCES platforms(id) NOT NULL,
  field_mapping JSONB NOT NULL,
  tables TEXT[] NOT NULL DEFAULT '{}',
  is_active BOOLEAN DEFAULT true,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Jobs tracking table
CREATE TABLE IF NOT EXISTS ingestion_jobs (
  id SERIAL PRIMARY KEY,
  integration_id INTEGER REFERENCES integrations(id) NOT NULL,
  file_path VARCHAR(500) NOT NULL,
  status VARCHAR(20) DEFAULT 'pending',
  total_rows INTEGER DEFAULT 0,
  processed_rows INTEGER DEFAULT 0,
  inserted_rows INTEGER DEFAULT 0,
  error_rows INTEGER DEFAULT 0,
  error_message TEXT,
  started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  completed_at TIMESTAMP
);

-- File tracking table
CREATE TABLE IF NOT EXISTS data_source_files (
  id SERIAL PRIMARY KEY,
  integration_id INTEGER REFERENCES integrations(id) NOT NULL,
  file_path VARCHAR(500) NOT NULL,
  file_hash VARCHAR(64) NOT NULL,
  total_rows INTEGER NOT NULL,
  job_id INTEGER REFERENCES ingestion_jobs(id),
  processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  UNIQUE(integration_id, file_hash)
);



-- Create indexes for performance
DO $$ BEGIN
    CREATE INDEX idx_orders_platform_id ON orders(platform_id);
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

DO $$ BEGIN
    CREATE INDEX idx_orders_restaurant_id ON orders(restaurant_id);
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

DO $$ BEGIN
    CREATE INDEX idx_orders_datetime ON orders(order_datetime);
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

DO $$ BEGIN
    CREATE INDEX idx_orders_status ON orders(order_status);
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

DO $$ BEGIN
    CREATE INDEX idx_restaurants_platform_id ON restaurants(platform_id);
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

DO $$ BEGIN
    CREATE INDEX idx_ratings_restaurant_id ON ratings(restaurant_id);
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

DO $$ BEGIN
    CREATE INDEX idx_ratings_platform_id ON ratings(platform_id);
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

-- Additional composite indexes
DO $$ BEGIN
    CREATE INDEX idx_orders_restaurant_date ON orders(restaurant_id, order_datetime);
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

DO $$ BEGIN
    CREATE INDEX idx_orders_platform_status ON orders(platform_id, order_status);
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

DO $$ BEGIN
    CREATE INDEX idx_orders_value_datetime ON orders(order_value, order_datetime);
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

DO $$ BEGIN
    CREATE INDEX idx_ratings_restaurant_date ON ratings(restaurant_id, rating_date);
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

-- Add check constraints
DO $$ BEGIN
    ALTER TABLE orders ADD CONSTRAINT chk_order_value_positive 
    CHECK (order_value >= 0);
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

DO $$ BEGIN
    ALTER TABLE orders ADD CONSTRAINT chk_basket_size_positive 
    CHECK (basket_size > 0);
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

DO $$ BEGIN
    ALTER TABLE orders ADD CONSTRAINT chk_discount_amount_valid 
    CHECK (discount_amount >= 0);
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

DO $$ BEGIN
    ALTER TABLE ratings ADD CONSTRAINT chk_rating_value_range 
    CHECK (rating_value >= 0 AND rating_value <= 5);
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

DO $$ BEGIN
    ALTER TABLE orders ADD CONSTRAINT chk_wait_times_positive 
    CHECK (restaurant_wait_time_minutes >= 0 AND 
           total_delivery_time_minutes >= 0 AND 
           courier_wait_time_minutes >= 0 AND 
           prep_time_minutes >= 0);
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;




