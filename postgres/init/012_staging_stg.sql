CREATE TABLE staging.stg_user (
    user_id            VARCHAR(64) NOT NULL,
    email              VARCHAR(255),
    phone              VARCHAR(50),
    full_name          VARCHAR(255),
    registration_ts    TIMESTAMP,
    status             VARCHAR(50),

    src_system         VARCHAR(50) NOT NULL DEFAULT 'oltp',
    src_created_at     TIMESTAMP,
    src_updated_at     TIMESTAMP,
    etl_batch_id       VARCHAR(64),
    etl_loaded_at      TIMESTAMP   NOT NULL DEFAULT NOW()
);

CREATE TABLE staging.stg_trip (
    trip_id              VARCHAR(64) NOT NULL,  
    user_id              VARCHAR(64) NOT NULL,
    route_id             VARCHAR(64),
    vehicle_id           VARCHAR(64),
    origin_stop_id       VARCHAR(64),
    destination_stop_id  VARCHAR(64),
    start_ts             TIMESTAMP,
    end_ts               TIMESTAMP,
    distance_meters      INTEGER,
    fare_product_id      VARCHAR(64),
    fare_amount          NUMERIC(10,2),
    currency_code        CHAR(3),
    trip_status          VARCHAR(50),
    created_at           TIMESTAMP,
    updated_at           TIMESTAMP,

    src_system           VARCHAR(50) NOT NULL DEFAULT 'oltp',
    etl_batch_id         VARCHAR(64),
    etl_loaded_at        TIMESTAMP   NOT NULL DEFAULT NOW()
);

CREATE TABLE staging.stg_payment (
    payment_id          VARCHAR(64) NOT NULL,   
    user_id             VARCHAR(64) NOT NULL,
    fare_product_id     VARCHAR(64),
    payment_method      VARCHAR(50),
    amount              NUMERIC(10,2),
    currency_code       CHAR(3),
    payment_status      VARCHAR(50),
    external_txn_id     VARCHAR(128),
    created_at          TIMESTAMP,
    updated_at          TIMESTAMP,

    src_system          VARCHAR(50) NOT NULL DEFAULT 'oltp',
    etl_batch_id        VARCHAR(64),
    etl_loaded_at       TIMESTAMP   NOT NULL DEFAULT NOW()
);


CREATE TABLE staging.stg_route (
    route_id              VARCHAR(64) NOT NULL,   
    route_code            VARCHAR(64) NOT NULL,
    route_name            VARCHAR(255),
    route_type            VARCHAR(50),
    description           TEXT,
    route_pattern_hash    VARCHAR(128),
    default_fare_product_id VARCHAR(64),

    valid_from_ts         TIMESTAMP,
    valid_to_ts           TIMESTAMP,
    is_active             BOOLEAN,

    src_system            VARCHAR(50) NOT NULL DEFAULT 'oltp',
    src_created_at        TIMESTAMP,
    src_updated_at        TIMESTAMP,
    etl_batch_id          VARCHAR(64),
    etl_loaded_at         TIMESTAMP   NOT NULL DEFAULT NOW()
);

CREATE TABLE staging.stg_stop (
    stop_id            VARCHAR(64) NOT NULL,
    stop_code          VARCHAR(64),
    stop_name          VARCHAR(255),
    city               VARCHAR(255),
    latitude           NUMERIC(9,6),
    longitude          NUMERIC(9,6),
    zone               VARCHAR(64),

    src_system         VARCHAR(50) NOT NULL DEFAULT 'oltp',
    src_created_at     TIMESTAMP,
    src_updated_at     TIMESTAMP,
    etl_batch_id       VARCHAR(64),
    etl_loaded_at      TIMESTAMP   NOT NULL DEFAULT NOW()
);

CREATE TABLE staging.stg_vehicle (
    vehicle_id            VARCHAR(64) NOT NULL,
    registration_number   VARCHAR(64),
    vehicle_type          VARCHAR(50),
    capacity              INTEGER,
    operator_name         VARCHAR(255),

    src_system            VARCHAR(50) NOT NULL DEFAULT 'oltp',
    src_created_at        TIMESTAMP,
    src_updated_at        TIMESTAMP,
    etl_batch_id          VARCHAR(64),
    etl_loaded_at         TIMESTAMP   NOT NULL DEFAULT NOW()
);

CREATE TABLE staging.stg_fare_product (
    fare_product_id      VARCHAR(64) NOT NULL,
    product_name         VARCHAR(255) NOT NULL,
    fare_type            VARCHAR(50),
    currency_code        CHAR(3),
    base_price           NUMERIC(10,2),
    description          TEXT,

    src_system           VARCHAR(50) NOT NULL DEFAULT 'oltp',
    src_created_at       TIMESTAMP,
    src_updated_at       TIMESTAMP,
    etl_batch_id         VARCHAR(64),
    etl_loaded_at        TIMESTAMP   NOT NULL DEFAULT NOW()
);

CREATE TABLE staging.stg_vehicle_position (
    raw_event_id        VARCHAR(128),        
    vehicle_id          VARCHAR(64) NOT NULL,
    route_id            VARCHAR(64),
    event_ts            TIMESTAMP   NOT NULL,
    latitude            NUMERIC(9,6) NOT NULL,
    longitude           NUMERIC(9,6) NOT NULL,
    speed_kmph          NUMERIC(6,2),
    heading_deg         NUMERIC(6,2),
    nearest_stop_id     VARCHAR(64),
    partition_id        INTEGER,
    offset_in_partition BIGINT,

    src_system          VARCHAR(50) NOT NULL DEFAULT 'kafka_vehicle_positions',
    etl_batch_id        VARCHAR(64),
    etl_loaded_at       TIMESTAMP   NOT NULL DEFAULT NOW()
);

