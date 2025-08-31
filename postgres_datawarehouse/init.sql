CREATE SCHEMA IF NOT EXISTS RAW;
CREATE SCHEMA IF NOT EXISTS BRONZE;
CREATE SCHEMA IF NOT EXISTS SILVER;
CREATE SCHEMA IF NOT EXISTS GOLD;


CREATE TABLE IF NOT EXISTS RAW.clients (
    id UUID ,
    name TEXT,
    email TEXT,
    country TEXT,
    created_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS RAW.suppliers (
    id UUID ,
    name TEXT,
    email TEXT,
    country TEXT,
    created_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS RAW.product (
    id UUID ,
    name TEXT,
    price NUMERIC,
    fournisseur_name TEXT,
    created_at TIMESTAMP
);

ALTER DATABASE dwh SET search_path = public, RAW, BRONZE, SILVER, GOLD;


