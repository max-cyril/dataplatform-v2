CREATE DATABASE IF NOT EXISTS records;
USE records;

CREATE TABLE clients (
    id BINARY(16) PRIMARY KEY,
    name VARCHAR(500),
    email VARCHAR(500),
    country VARCHAR(500),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE suppliers (
    id BINARY(16) PRIMARY KEY,
    name VARCHAR(500),
    email VARCHAR(500),
    country VARCHAR(500),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE product (
    id BINARY(16) PRIMARY KEY,
    name VARCHAR(100),
    price DECIMAL(10,2),
    fournisseur_name VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
