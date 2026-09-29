SELECT
    CASE WHEN cost >= 670 THEN 'HIGH' ELSE 'LOW' END AS cost_tier,
    CASE WHEN price >= 1150 THEN 'PREMIUM' ELSE 'STANDARD' END AS price_tier,
    STDDEV_SAMP(price) AS price_stddev,
    PERCENTILE_APPROX(price, ARRAY(0.25, 0.5, 0.75)) AS price_quartiles,
    BIT_OR(stock_qty) AS stock_qty_bit_or,
    COUNT_IF(brand = 'Umbrella') AS umbrella_events
FROM products
GROUP BY CASE WHEN cost >= 670 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN price >= 1150 THEN 'PREMIUM' ELSE 'STANDARD' END
HAVING BIT_OR(stock_qty) > 1;
