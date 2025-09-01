CREATE SCHEMA IF NOT EXISTS RAW;
CREATE SCHEMA IF NOT EXISTS BRONZE;
CREATE SCHEMA IF NOT EXISTS SILVER;
CREATE SCHEMA IF NOT EXISTS GOLD;


CREATE TABLE IF NOT EXISTS RAW.clients (
    id UUID PRIMARY KEY,
    name TEXT,
    email TEXT,
    country TEXT,
    created_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS RAW.suppliers (
    id UUID PRIMARY KEY,
    name TEXT,
    email TEXT,
    country TEXT,
    created_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS RAW.product (
    id UUID  PRIMARY KEY,
    name TEXT,
    price NUMERIC,
    supplier_name TEXT,
    created_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS RAW.orders (
    id UUID PRIMARY KEY,
    client_id UUID,
    product_id UUID,
    quantity INT,
    total_price DECIMAL(10,2),
    order_date TIMESTAMP
);

ALTER DATABASE dwh SET search_path = public, RAW, BRONZE, SILVER, GOLD;


