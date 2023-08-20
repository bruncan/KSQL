--Carga el archivo de docker-compose.yml
docker-compose up -d

--Inicia ksqlDB
docker exec -it ksqldb-cli ksql http://ksqldb-server:8088

--Da de baja el docker
docker-compose down

--Se crea el Stream para almacenar los resultados del Consumer en KSQL

CREATE STREAM finnhub_trades_stream (
    symbol STRING,
    price DOUBLE,
    volume DOUBLE,
    timestamp STRING
) WITH (
    KAFKA_TOPIC='finnhub-trades',
    VALUE_FORMAT='JSON'
);

--1. ¿Cuál fue el promedio ponderado de precio de una unidad por cada uno de los símbolos procesados? (e.j. AAPL)
--Se crea una tabla auxiliar para el calculo del promedio
CREATE TABLE promedio_ponderado AS
    SELECT 
        symbol,
        SUM(price) / SUM(volume) AS promedio_ponderado_precio
    FROM finnhub_trades_stream
    WHERE price IS NOT NULL AND volume IS NOT NULL
    GROUP BY symbol
    EMIT CHANGES;

--Consulta para obtener los resultados de la tabla
SELECT * FROM promedio_ponderado;

--2.¿Cuántas transacciones se procesaron por símbolo? 

CREATE TABLE transacciones_por_simbolo AS
    SELECT 
        symbol,
        COUNT(*) AS cantidad_de_transacciones
    FROM finnhub_trades_stream
    GROUP BY symbol
    EMIT CHANGES;

SELECT * FROM transacciones_por_simbolo;

--3.¿Cuál fue el máximo precio registrado por símbolo? 
CREATE TABLE maximo_precio_por_simbolo AS
    SELECT 
        symbol,
        MAX(price) AS maximo_precio
    FROM finnhub_trades_stream
    GROUP BY symbol
    EMIT CHANGES;

SELECT * FROM maximo_precio_por_simbolo;

--4.¿Cuál fue el mínimo precio registrado por símbolo?
CREATE TABLE minimo_precio_por_simbolo AS
    SELECT 
        symbol,
        MIN(price) AS minimo_precio
    FROM finnhub_trades_stream
    GROUP BY symbol
    EMIT CHANGES;

SELECT * FROM minimo_precio_por_simbolo;