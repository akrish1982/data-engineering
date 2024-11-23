-- Poorly designed Date dimension table
CREATE TABLE date_dim (
    date_key BIGINT,  
    date_actual DATE,
    year VARCHAR(10), -- Is this the right data type? 
    quarter CHAR(2), -- Is this the right data type?
    month CHAR(2), -- Is this the right data type?
    week VARCHAR(10), -- Is this the right data type? 
    day_of_week VARCHAR(10), -- Is this the right data type? 
    day_of_month VARCHAR(10) -- Is this the right data type? 
)
DISTSTYLE ALL; -- Using ALL for distribution, causing data duplication

-- Poorly designed Customer dimension table
CREATE TABLE customers (
    customer_id VARCHAR(50), -- Is this the right data type? 
    email VARCHAR(255), -- Overly large size
    first_name VARCHAR(255), -- Overly large size
    last_name VARCHAR(255), -- Overly large size
    address VARCHAR(500), -- Overly large size
    city VARCHAR(255), -- Overly large size
    country VARCHAR(255), -- Overly large size
    created_at TIMESTAMP
)
DISTSTYLE ALL -- Using ALL for distribution, causing data duplication
SORTKEY (email); -- Irrelevant sort key for query performance

-- Poorly designed Product dimension table
CREATE TABLE products (
    product_id VARCHAR(50), -- Is this the right data type? 
    name VARCHAR(1000), -- Overly large size
    description TEXT, -- Using TEXT 
    category VARCHAR(500), -- Overly large size
    price FLOAT, -- HINT. FLOAT is less precise than DECIMAL
    cost FLOAT, -- HINT. FLOAT is less precise than DECIMAL
    supplier_id VARCHAR(50), -- Is this the right data type? 
    created_at DATE -- Changed from TIMESTAMP 
)
SORTKEY (category); -- Irrelevant sort key for query performance

-- Poorly designed Orders fact table
CREATE TABLE orders (
    order_id BIGINT,  
    customer_id VARCHAR(50), -- Is this the right data type? 
    order_date_key VARCHAR(50), -- Is this the right data type? 
    status VARCHAR(500), -- Overly large size
    total_amount FLOAT, -- HINT. FLOAT is less precise than DECIMAL
    shipping_address TEXT, -- Using TEXT 
    shipping_city TEXT, -- Using TEXT 
    shipping_country TEXT -- Using TEXT 
)
DISTSTYLE EVEN; -- Using EVEN distribution, which can lead to skewed joins
-- No sort keys, reducing query performance

-- Poorly designed Order items fact table
CREATE TABLE order_items (
    order_item_id VARCHAR(50), -- Is this the right data type? 
    order_id BIGINT,  
    product_id VARCHAR(50), -- Is this the right data type? 
    quantity FLOAT, -- Changed to FLOAT 
    price FLOAT, -- HINT. FLOAT is less precise than DECIMAL
    discount FLOAT DEFAULT 0,
    total_amount FLOAT -- HINT. FLOAT is less precise than DECIMAL
)
DISTSTYLE EVEN -- Using EVEN distribution, which can lead to skewed joins
SORTKEY (discount); -- Irrelevant sort key for query performance
