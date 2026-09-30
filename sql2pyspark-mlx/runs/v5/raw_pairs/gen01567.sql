WITH latest AS (
    SELECT *
    FROM products
    QUALIFY RANK() OVER (PARTITION BY category, brand ORDER BY cost) = 1
)
SELECT
    CASE WHEN cost >= 750 THEN 'PREMIUM' ELSE 'STANDARD' END AS cost_tier,
    DATE_TRUNC('WEEK', launch_date) AS week_start,
    ROUND(AVG(price), 2) AS avg_price,
    APPROX_PERCENTILE(price, 0.9) AS price_p90,
    BIT_AND(stock_qty) AS stock_qty_bit_and
FROM latest
GROUP BY ROLLUP (CASE WHEN cost >= 750 THEN 'PREMIUM' ELSE 'STANDARD' END, DATE_TRUNC('WEEK', launch_date))
