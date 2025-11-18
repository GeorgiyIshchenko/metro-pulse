CREATE TABLE dwh.fact_trip (
    trip_id                BIGSERIAL PRIMARY KEY,
    user_sk                BIGINT    NOT NULL,
    route_sk               BIGINT    NOT NULL,
    vehicle_sk             BIGINT,
    origin_stop_sk         BIGINT,
    destination_stop_sk    BIGINT,
    start_date_key         INTEGER   NOT NULL,
    start_time_key         INTEGER   NOT NULL,
    end_date_key           INTEGER,
    end_time_key           INTEGER,
    fare_product_sk        BIGINT,

    trip_duration_sec      INTEGER,
    distance_meters        INTEGER,
    fare_amount            NUMERIC(10,2),
    currency_code          CHAR(3)   NOT NULL DEFAULT 'EUR',

    trip_status            VARCHAR(50),   -- completed, canceled, ongoing...
    created_at             TIMESTAMP,     
    etl_loaded_at          TIMESTAMP NOT NULL DEFAULT NOW(),

    CONSTRAINT fk_fact_trip_user
        FOREIGN KEY (user_sk) REFERENCES dwh.dim_user (user_sk),
    CONSTRAINT fk_fact_trip_route
        FOREIGN KEY (route_sk) REFERENCES dwh.dim_route (route_sk),
    CONSTRAINT fk_fact_trip_vehicle
        FOREIGN KEY (vehicle_sk) REFERENCES dwh.dim_vehicle (vehicle_sk),
    CONSTRAINT fk_fact_trip_origin_stop
        FOREIGN KEY (origin_stop_sk) REFERENCES dwh.dim_stop (stop_sk),
    CONSTRAINT fk_fact_trip_destination_stop
        FOREIGN KEY (destination_stop_sk) REFERENCES dwh.dim_stop (stop_sk),
    CONSTRAINT fk_fact_trip_start_date
        FOREIGN KEY (start_date_key) REFERENCES dwh.dim_date (date_key),
    CONSTRAINT fk_fact_trip_start_time
        FOREIGN KEY (start_time_key) REFERENCES dwh.dim_time (time_key),
    CONSTRAINT fk_fact_trip_end_date
        FOREIGN KEY (end_date_key) REFERENCES dwh.dim_date (date_key),
    CONSTRAINT fk_fact_trip_end_time
        FOREIGN KEY (end_time_key) REFERENCES dwh.dim_time (time_key),
    CONSTRAINT fk_fact_trip_fare_product
        FOREIGN KEY (fare_product_sk) REFERENCES dwh.dim_fare_product (fare_product_sk)
);

CREATE TABLE dwh.fact_payment (
    payment_id            BIGSERIAL PRIMARY KEY,
    user_sk               BIGINT    NOT NULL,
    fare_product_sk       BIGINT,
    payment_date_key      INTEGER   NOT NULL,
    payment_time_key      INTEGER   NOT NULL,

    payment_method        VARCHAR(50),       
    amount                NUMERIC(10,2) NOT NULL,
    currency_code         CHAR(3)      NOT NULL DEFAULT 'EUR',
    payment_status        VARCHAR(50),       
    external_txn_id       VARCHAR(128),      
    created_at            TIMESTAMP,
    etl_loaded_at         TIMESTAMP   NOT NULL DEFAULT NOW(),

    CONSTRAINT fk_fact_payment_user
        FOREIGN KEY (user_sk) REFERENCES dwh.dim_user (user_sk),
    CONSTRAINT fk_fact_payment_fare_product
        FOREIGN KEY (fare_product_sk) REFERENCES dwh.dim_fare_product (fare_product_sk),
    CONSTRAINT fk_fact_payment_date
        FOREIGN KEY (payment_date_key) REFERENCES dwh.dim_date (date_key),
    CONSTRAINT fk_fact_payment_time
        FOREIGN KEY (payment_time_key) REFERENCES dwh.dim_time (time_key)
);


CREATE TABLE dwh.fact_vehicle_position (
    vehicle_position_id   BIGSERIAL PRIMARY KEY,
    vehicle_sk            BIGINT    NOT NULL,
    route_sk              BIGINT,
    date_key              INTEGER   NOT NULL,
    time_key              INTEGER   NOT NULL,
    latitude              NUMERIC(9,6)  NOT NULL,
    longitude             NUMERIC(9,6)  NOT NULL,
    speed_kmph            NUMERIC(6,2),
    heading_deg           NUMERIC(6,2),

    nearest_stop_sk       BIGINT,

    -- raw_event_id          VARCHAR(128),   
    etl_loaded_at         TIMESTAMP NOT NULL DEFAULT NOW(),

    CONSTRAINT fk_fact_vehicle_position_vehicle
        FOREIGN KEY (vehicle_sk) REFERENCES dwh.dim_vehicle (vehicle_sk),
    CONSTRAINT fk_fact_vehicle_position_route
        FOREIGN KEY (route_sk) REFERENCES dwh.dim_route (route_sk),
    CONSTRAINT fk_fact_vehicle_position_date
        FOREIGN KEY (date_key) REFERENCES dwh.dim_date (date_key),
    CONSTRAINT fk_fact_vehicle_position_time
        FOREIGN KEY (time_key) REFERENCES dwh.dim_time (time_key),
    CONSTRAINT fk_fact_vehicle_position_stop
        FOREIGN KEY (nearest_stop_sk) REFERENCES dwh.dim_stop (stop_sk)
);
