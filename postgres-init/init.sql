CREATE TABLE IF NOT EXISTS country (
    id SERIAL PRIMARY KEY,
    code VARCHAR(10) NOT NULL,
    name VARCHAR(255) NOT NULL,
    "datetimeFirst" TIMESTAMP,
    "datetimeLast" TIMESTAMP
);

CREATE TABLE IF NOT EXISTS parameter (
    parameter_id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    unit VARCHAR(50),
    display_name VARCHAR(255),
    description TEXT
);

CREATE TABLE IF NOT EXISTS location_table (
    location_id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    locality VARCHAR(255),
    country_id INT REFERENCES country(id),
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    timezone VARCHAR(50),
    datetime_first_utc TIMESTAMP,
    datetime_first_local TIMESTAMP,
    datetime_last_utc TIMESTAMP,
    datetime_last_local TIMESTAMP
);

CREATE TABLE IF NOT EXISTS sensor (
    sensor_id SERIAL PRIMARY KEY,
    name VARCHAR(255),
    parameter_id INT REFERENCES parameter(parameter_id),
    location_id INT REFERENCES location_table(location_id)
);

CREATE TABLE IF NOT EXISTS latest_measurements (
    latest_id VARCHAR(255) PRIMARY KEY,
    location_id INT REFERENCES location_table(location_id),
    sensor_id INT REFERENCES sensor(sensor_id),
    value DOUBLE PRECISION,
    timestamp_utc TIMESTAMP,
    timestamp_local TIMESTAMP,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION
);