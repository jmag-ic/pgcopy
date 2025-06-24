-- Test DDL for pgcopy integration tests
-- Contains various PostgreSQL column types including JSONB and arrays

-- Enable extensions
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- Create test schemas
CREATE SCHEMA IF NOT EXISTS public;
CREATE SCHEMA IF NOT EXISTS analytics;

-- Users table with common types
CREATE TABLE public.users (
  id SERIAL PRIMARY KEY,
  username VARCHAR(50) UNIQUE NOT NULL,
  email VARCHAR(255) UNIQUE NOT NULL,
  password_hash VARCHAR(255) NOT NULL,
  first_name VARCHAR(100),
  last_name VARCHAR(100),
  is_active BOOLEAN DEFAULT true,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
  last_login TIMESTAMP WITH TIME ZONE,
  login_count INTEGER DEFAULT 0,
  profile_data JSONB,
  preferences JSONB DEFAULT '{}',
  tags TEXT[],
  roles VARCHAR(50)[] DEFAULT '{}',
  metadata JSONB DEFAULT '{}'
);

-- Products table with numeric types
CREATE TABLE public.products (
  id SERIAL PRIMARY KEY,
  name VARCHAR(200) NOT NULL,
  description TEXT,
  price DECIMAL(10,2) NOT NULL,
  cost DECIMAL(10,2),
  weight_kg NUMERIC(8,3),
  dimensions_cm NUMERIC(8,2)[],
  category VARCHAR(100),
  tags TEXT[],
  attributes JSONB,
  is_available BOOLEAN DEFAULT true,
  stock_quantity INTEGER DEFAULT 0,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Orders table with foreign keys and complex types
CREATE TABLE public.orders (
  id SERIAL PRIMARY KEY,
  user_id INTEGER REFERENCES public.users(id),
  order_number VARCHAR(50) UNIQUE NOT NULL,
  status VARCHAR(20) DEFAULT 'pending',
  total_amount DECIMAL(12,2) NOT NULL,
  tax_amount DECIMAL(12,2) DEFAULT 0,
  shipping_address JSONB,
  billing_address JSONB,
  items JSONB,
  payment_info JSONB,
  notes TEXT,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
  shipped_at TIMESTAMP WITH TIME ZONE,
  delivered_at TIMESTAMP WITH TIME ZONE
);

-- Analytics table with time series data
CREATE TABLE analytics.page_views (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id INTEGER,
  page_url VARCHAR(500) NOT NULL,
  page_title VARCHAR(200),
  referrer VARCHAR(500),
  user_agent TEXT,
  ip_address INET,
  session_id VARCHAR(100),
  view_duration_seconds INTEGER,
  scroll_depth_percent INTEGER,
  interactions JSONB,
  custom_events JSONB,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Complex data types table
CREATE TABLE public.complex_data (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    
    -- Array types
    string_array TEXT[],
    int_array INTEGER[],
    float_array REAL[],
    bool_array BOOLEAN[],
    date_array DATE[],
    timestamp_array TIMESTAMP WITH TIME ZONE[],
    
    -- JSONB types
    simple_json JSONB,
    nested_json JSONB,
    array_json JSONB,
    
    -- Geometric types
    point_coord POINT,
    line_coords LINE,
    polygon_coords POLYGON,
    
    -- Network types
    ip_addr INET,
    mac_addr MACADDR,
    cidr_range CIDR,
    
    -- Text search
    search_vector TSVECTOR,
    
    -- Binary data
    binary_data BYTEA,
    
    -- UUID
    unique_id UUID DEFAULT gen_random_uuid(),
    
    -- Money
    price MONEY,
    
    -- Bit strings
    bit_field BIT(8),
    var_bit_field BIT VARYING(16),
    
    -- XML
    xml_data XML,
    
    -- Range types
    int_range INT4RANGE,
    date_range DATERANGE,
    timestamp_range TSRANGE,
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better performance
CREATE INDEX idx_users_email ON public.users(email);
CREATE INDEX idx_users_username ON public.users(username);
CREATE INDEX idx_users_created_at ON public.users(created_at);
CREATE INDEX idx_users_profile_data ON public.users USING GIN(profile_data);

CREATE INDEX idx_products_category ON public.products(category);
CREATE INDEX idx_products_price ON public.products(price);
CREATE INDEX idx_products_attributes ON public.products USING GIN(attributes);

CREATE INDEX idx_orders_user_id ON public.orders(user_id);
CREATE INDEX idx_orders_status ON public.orders(status);
CREATE INDEX idx_orders_created_at ON public.orders(created_at);

CREATE INDEX idx_page_views_user_id ON analytics.page_views(user_id);
CREATE INDEX idx_page_views_created_at ON analytics.page_views(created_at);
CREATE INDEX idx_page_views_page_url ON analytics.page_views(page_url);
