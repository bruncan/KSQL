SELECT
    symbol,
    SUM(price * volume) / SUM(volume) AS promedio_ponderado
FROM
    finnhub_trades_stream
GROUP BY
    symbol EMIT CHANGES;
