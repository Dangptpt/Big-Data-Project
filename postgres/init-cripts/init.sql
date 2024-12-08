-- Tạo bảng crime
CREATE TABLE IF NOT EXISTS crime (
    id SERIAL PRIMARY KEY,
    occ_date TIMESTAMP,
    report_date TIMESTAMP,
    premises_type VARCHAR(50),
    bike_cost DOUBLE PRECISION,
    bike_speed DOUBLE PRECISION,
    bike_make VARCHAR(50),
    bike_model VARCHAR(50)
);

-- Tạo bảng premise
CREATE TABLE IF NOT EXISTS premise (
    id SERIAL PRIMARY KEY,
    type VARCHAR(50),
    total_cases INT,
    avg_value DOUBLE PRECISION,
    year INT,
    month INT
);

-- Tạo bảng temporal
CREATE TABLE IF NOT EXISTS temporal (
    id SERIAL PRIMARY KEY,
    total_cases INT,
    hour INT,
    day_of_week INT,
    avg_value DOUBLE PRECISION,
    year INT
);

-- Tạo bảng division
CREATE TABLE IF NOT EXISTS division (
    id SERIAL PRIMARY KEY,
    division VARCHAR(50),
    premise_type VARCHAR(50),
    total_cases INT,
    year INT,
    avg_value DOUBLE PRECISION
);

-- Tạo bảng neighbourhood
CREATE TABLE IF NOT EXISTS neighbourhood (
    id SERIAL PRIMARY KEY,
    type VARCHAR(50),
    total_cases INT,
    year INT,
    avg_value DOUBLE PRECISION,
    month INT
);
