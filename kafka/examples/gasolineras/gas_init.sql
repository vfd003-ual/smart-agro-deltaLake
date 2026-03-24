-- 1. Activamos las extensiones
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- 2. Tabla de datos de la gasolinera
CREATE TABLE IF NOT EXISTS gasolineras (
    ideess VARCHAR(20) PRIMARY KEY,
    rotulo VARCHAR(150),
    direccion VARCHAR(250),
    cp VARCHAR(10),
    horario VARCHAR(250),
    latitud NUMERIC,
    longitud NUMERIC,
    localidad VARCHAR(150),
    municipio VARCHAR(150),
    provincia VARCHAR(150),
    id_municipio VARCHAR(10),
    id_provincia VARCHAR(10),
    id_ccaa VARCHAR(10),
    margen VARCHAR(10),
    remision VARCHAR(10),
    tipo_venta VARCHAR(10),
    porcentaje_bioetanol NUMERIC,
    porcentaje_ester_metilico NUMERIC
);

-- 3. Tabla de precios
CREATE TABLE IF NOT EXISTS precios (
    ideess VARCHAR(20) REFERENCES gasolineras(ideess),
    fecha_consulta TIMESTAMP,
    precio_adblue NUMERIC,
    precio_amoniaco NUMERIC,
    precio_biodiesel NUMERIC,
    precio_bioetanol NUMERIC,
    precio_biogas_natural_comprimido NUMERIC,
    precio_biogas_natural_licuado NUMERIC,
    precio_diesel_renovable NUMERIC,
    precio_gas_natural_comprimido NUMERIC,
    precio_gas_natural_licuado NUMERIC,
    precio_gases_licuados_petroleo NUMERIC,
    precio_gasoleo_a NUMERIC,
    precio_gasoleo_b NUMERIC,
    precio_gasoleo_premium NUMERIC,
    precio_gasolina_95_e10 NUMERIC,
    precio_gasolina_95_e25 NUMERIC,
    precio_gasolina_95_e5 NUMERIC,
    precio_gasolina_95_e5_premium NUMERIC,
    precio_gasolina_95_e85 NUMERIC,
    precio_gasolina_98_e10 NUMERIC,
    precio_gasolina_98_e5 NUMERIC,
    precio_gasolina_renovable NUMERIC,
    precio_hidrogeno NUMERIC,
    precio_metanol NUMERIC,
    PRIMARY KEY (ideess, fecha_consulta)
);

-- 4. Convertimos a Hypertable para TimescaleDB
SELECT create_hypertable('precios', 'fecha_consulta', if_not_exists => TRUE);