-- Date dimension table
CREATE TABLE date_dim (
    date_key INT PRIMARY KEY DISTKEY,
    date_actual DATE NOT NULL,
    year INT NOT NULL,
    quarter INT NOT NULL,
    month INT NOT NULL,
    week INT NOT NULL,
    day_of_week INT NOT NULL,
    day_of_month INT NOT NULL
) SORTKEY (date_actual);

-- Customer dimension table
CREATE TABLE customers (
    customer_id INT PRIMARY KEY DISTKEY,
    email VARCHAR(100) NOT NULL,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    address VARCHAR(200),
    city VARCHAR(100),
    country VARCHAR(100),
    created_at TIMESTAMP NOT NULL
) SORTKEY (customer_id);

-- Product dimension table
CREATE TABLE products (
    product_id INT PRIMARY KEY DISTKEY,
    name VARCHAR(200) NOT NULL,
    description VARCHAR(1000),
    category VARCHAR(100),
    price DECIMAL(10,2) NOT NULL,
    cost DECIMAL(10,2) NOT NULL,
    supplier_id INT,
    created_at TIMESTAMP NOT NULL
) SORTKEY (product_id);

-- Orders fact table
CREATE TABLE orders (
    order_id INT PRIMARY KEY,
    customer_id INT NOT NULL DISTKEY REFERENCES customers(customer_id),
    order_date_key INT NOT NULL REFERENCES date_dim(date_key),
    status VARCHAR(50) NOT NULL,
    total_amount DECIMAL(12,2) NOT NULL,
    shipping_address VARCHAR(200),
    shipping_city VARCHAR(100),
    shipping_country VARCHAR(100)
) SORTKEY (order_date_key, customer_id);

-- Order items fact table
CREATE TABLE order_items (
    order_item_id INT PRIMARY KEY,
    order_id INT NOT NULL REFERENCES orders(order_id),
    product_id INT NOT NULL REFERENCES products(product_id),
    quantity INT NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    discount DECIMAL(10,2) DEFAULT 0,
    total_amount DECIMAL(12,2) NOT NULL
) DISTKEY(product_id) SORTKEY(order_id, product_id);