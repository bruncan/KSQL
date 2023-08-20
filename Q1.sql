Create table promedio_ponderado2 AS
    SELECT
        symbol,
        SUM(price) / Count(*) AS promedio_ponderado
    FROM
        finnhub_trades_stream
    GROUP BY
        symbol
    EMIT CHANGES;