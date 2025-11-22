CREATE TABLE dwh.dim_date (
    date_key        INTEGER PRIMARY KEY,    -- формат YYYYMMDD
    full_date       DATE        NOT NULL,
    year            SMALLINT    NOT NULL,
    quarter         SMALLINT    NOT NULL,
    month           SMALLINT    NOT NULL,
    month_name      VARCHAR(20) NOT NULL,
    week_of_year    SMALLINT    NOT NULL,
    day_of_month    SMALLINT    NOT NULL,
    day_of_week     SMALLINT    NOT NULL,   -- 1=Monday ... 7=Sunday
    day_name        VARCHAR(20) NOT NULL,
    is_weekend      BOOLEAN     NOT NULL DEFAULT FALSE
);

CREATE TABLE dwh.dim_time (
    time_key        INTEGER PRIMARY KEY,   -- формат HHMMSS
    full_time       TIME        NOT NULL,
    hour            SMALLINT    NOT NULL,
    minute          SMALLINT    NOT NULL,
    second          SMALLINT    NOT NULL
);

CREATE TABLE dwh.dim_user (
    user_sk         BIGSERIAL   PRIMARY KEY,
    user_id_nat     VARCHAR(64) NOT NULL,          
    user_uuid       UUID,                          
    email           VARCHAR(255),
    phone           VARCHAR(50),
    full_name       VARCHAR(255),
    registration_dt DATE,
    status          VARCHAR(50),

    src_system      VARCHAR(50) NOT NULL DEFAULT 'oltp',
    etl_loaded_at   TIMESTAMP   NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_dim_user_business UNIQUE (user_id_nat)
);

CREATE TABLE dwh.dim_vehicle (
    vehicle_sk          BIGSERIAL   PRIMARY KEY,
    vehicle_id_nat      VARCHAR(64) NOT NULL,        
    registration_number VARCHAR(64),
    vehicle_type        VARCHAR(50),                 -- bus/tram/metro
    capacity            INTEGER,
    operator_name       VARCHAR(255),

    src_system          VARCHAR(50) NOT NULL DEFAULT 'oltp_or_stream',
    etl_loaded_at       TIMESTAMP   NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_dim_vehicle_business UNIQUE (vehicle_id_nat)
);

CREATE TABLE dwh.dim_stop (
    stop_sk        BIGSERIAL   PRIMARY KEY,
    stop_id_nat    VARCHAR(64) NOT NULL,          
    stop_code      VARCHAR(64),
    stop_name      VARCHAR(255),
    city           VARCHAR(255),
    latitude       NUMERIC(9,6),
    longitude      NUMERIC(9,6),
    zone           VARCHAR(64),

    src_system     VARCHAR(50) NOT NULL DEFAULT 'oltp',
    etl_loaded_at  TIMESTAMP   NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_dim_stop_business UNIQUE (stop_id_nat)
);

CREATE TABLE dwh.dim_fare_product (
    fare_product_sk      BIGSERIAL   PRIMARY KEY,
    fare_product_id_nat  VARCHAR(64) NOT NULL,     
    product_name         VARCHAR(255) NOT NULL,
    fare_type            VARCHAR(50),              -- single, pass, etc.
    currency_code        CHAR(3) NOT NULL DEFAULT 'EUR',
    base_price           NUMERIC(10,2),
    description          TEXT,

    src_system           VARCHAR(50) NOT NULL DEFAULT 'oltp',
    etl_loaded_at        TIMESTAMP   NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_dim_fare_product_business UNIQUE (fare_product_id_nat)
);


CREATE TABLE dwh.dim_route (
    route_sk            BIGSERIAL   PRIMARY KEY,    
    route_id_nat        VARCHAR(64) NOT NULL,      
    route_code          VARCHAR(64) NOT NULL,       
    route_name          VARCHAR(255),
    route_type          VARCHAR(50),                -- bus/tram/metro
    description         TEXT,
    route_pattern_hash  VARCHAR(128),
    default_fare_product_sk BIGINT,

    valid_from          DATE        NOT NULL,
    valid_to            DATE        NOT NULL,
    is_current          BOOLEAN     NOT NULL DEFAULT TRUE,

    src_system          VARCHAR(50) NOT NULL DEFAULT 'oltp',
    etl_loaded_at       TIMESTAMP   NOT NULL DEFAULT NOW(),
    CONSTRAINT fk_dim_route_fare_product
        FOREIGN KEY (default_fare_product_sk)
        REFERENCES dwh.dim_fare_product (fare_product_sk),
    CONSTRAINT uq_dim_route_bk_valid_from UNIQUE (route_id_nat, valid_from)
);

