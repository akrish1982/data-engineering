-- Poorly designed Date dimension table
CREATE TABLE date_dim (
    date_key BIGINT, -- Changed to BIGINT unnecessarily
    date_actual DATE,
    year VARCHAR(10), -- Changed to VARCHAR unnecessarily
    quarter CHAR(2), -- Changed to CHAR unnecessarily
    month CHAR(2), -- Changed to CHAR unnecessarily
    week VARCHAR(10), -- Changed to VARCHAR unnecessarily
    day_of_week VARCHAR(10), -- Changed to VARCHAR unnecessarily
    day_of_month VARCHAR(10) -- Changed to VARCHAR unnecessarily
)
DISTSTYLE ALL; -- Using ALL for distribution, causing data duplication

-- Poorly designed Customer dimension table
CREATE TABLE customers (
    customer_id VARCHAR(50), -- Changed to VARCHAR unnecessarily
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
    product_id VARCHAR(50), -- Changed to VARCHAR unnecessarily
    name VARCHAR(1000), -- Overly large size
    description TEXT, -- Using TEXT unnecessarily
    category VARCHAR(500), -- Overly large size
    price FLOAT, -- Less precise than DECIMAL
    cost FLOAT, -- Less precise than DECIMAL
    supplier_id VARCHAR(50), -- Changed to VARCHAR unnecessarily
    created_at DATE -- Changed from TIMESTAMP unnecessarily
)
SORTKEY (category); -- Irrelevant sort key for query performance

-- Poorly designed Orders fact table
CREATE TABLE orders (
    order_id BIGINT, -- Changed to BIGINT unnecessarily
    customer_id VARCHAR(50), -- Changed to VARCHAR unnecessarily
    order_date_key VARCHAR(50), -- Changed to VARCHAR unnecessarily
    status VARCHAR(500), -- Overly large size
    total_amount FLOAT, -- Less precise than DECIMAL
    shipping_address TEXT, -- Using TEXT unnecessarily
    shipping_city TEXT, -- Using TEXT unnecessarily
    shipping_country TEXT -- Using TEXT unnecessarily
)
DISTSTYLE EVEN; -- Using EVEN distribution, which can lead to skewed joins
-- No sort keys, reducing query performance

-- Poorly designed Order items fact table
CREATE TABLE order_items (
    order_item_id VARCHAR(50), -- Changed to VARCHAR unnecessarily
    order_id BIGINT, -- Changed to BIGINT unnecessarily
    product_id VARCHAR(50), -- Changed to VARCHAR unnecessarily
    quantity FLOAT, -- Changed to FLOAT unnecessarily
    price FLOAT, -- Less precise than DECIMAL
    discount FLOAT DEFAULT 0,
    total_amount FLOAT -- Less precise than DECIMAL
)
DISTSTYLE EVEN -- Using EVEN distribution, which can lead to skewed joins
SORTKEY (discount); -- Irrelevant sort key for query performance
