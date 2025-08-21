CREATE DATABASE IF NOT EXISTS streams;
USE streams;

CREATE TABLE orders (
    id BINARY(16) PRIMARY KEY,
    client_id BINARY(16),
    product_id BINARY(16),
    quantity INT,
    total_price DECIMAL(10,2),
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
