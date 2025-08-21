CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE raw.clients (
    id UUID PRIMARY KEY,
    name TEXT,
    email TEXT,
    country TEXT,
    created_at TIMESTAMP
);

CREATE TABLE raw.suppliers (
    id UUID PRIMARY KEY,
    name TEXT,
    email TEXT,
    country TEXT,
    created_at TIMESTAMP
);

CREATE TABLE raw.product (
    id UUID PRIMARY KEY,
    name TEXT,
    price NUMERIC,
    fournisseur_name TEXT,
    created_at TIMESTAMP
);

CREATE TABLE raw.orders (
    id UUID PRIMARY KEY,
    client_id UUID,
    product_id UUID,
    quantity INT,
    total_price NUMERIC,
    order_date TIMESTAMP
);
