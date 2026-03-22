-- Drop the database if it already exists
DROP DATABASE IF EXISTS gans_local ;

-- Create the database
CREATE DATABASE gans_local;

-- Use the database
USE gans_local;


CREATE TABLE city_info (
    city_id INT PRIMARY KEY AUTO_INCREMENT,
    city_name VARCHAR(100) NOT NULL,
    country VARCHAR(100) NOT NULL,
    UNIQUE (city_name , country),
    latitude FLOAT NOT NULL,
    longitude FLOAT NOT NULL
);
CREATE TABLE city_population (
    city_id INT,
    timestamp_population DATE NOT NULL,
    population INT NOT NULL,
    FOREIGN KEY (city_id) REFERENCES city_info (city_id),
    UNIQUE (city_id , timestamp_population)

);
CREATE TABLE weather_data (
    city_id INT,
    forecast_time DATETIME,
    outlook VARCHAR(255),
    temperature FLOAT,
    feels_like FLOAT,
    wind_speed FLOAT,
    rain_prob FLOAT,
    rain_in_last_3h FLOAT,
    data_retrieved_at DATETIME,
    FOREIGN KEY (city_id)
        REFERENCES city_info (city_id),
    UNIQUE (city_id , forecast_time, data_retrieved_at)    
);
CREATE TABLE airport_info (
    city_id INT,
    icao_info VARCHAR(10),
    iata_info VARCHAR(5),
    airport_name VARCHAR(255),
    airport_municipality VARCHAR(50),
    airport_timezone VARCHAR(150),
    UNIQUE(icao_info),
    FOREIGN KEY (city_id)
        REFERENCES city_info (city_id)
);
CREATE TABLE flights_info (
    arrival_airport_icao VARCHAR(10),
    departure_airport_icao VARCHAR(10),
    scheduled_arrival_time DATETIME,
    flight_number VARCHAR(20) ,
    data_retrieved_at DATETIME,
    FOREIGN KEY (arrival_airport_icao)
        REFERENCES airport_info(icao_info)

); 

