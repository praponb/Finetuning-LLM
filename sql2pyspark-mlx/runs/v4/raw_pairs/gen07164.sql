WITH base AS (
    SELECT *, COALESCE(unit_price, 0) AS unit_price_filled
    FROM order_items
    WHERE unit_price >= 260
)
SELECT
    CASE WHEN unit_price_filled >= 61 THEN 'PREMIUM' ELSE 'STANDARD' END AS unit_price_filled_tier,
    CASE WHEN quantity >= 9 THEN 'LARGE' ELSE 'SMALL' END AS quantity_tier,
    PERCENTILE_APPROX(unit_price_filled, 0.9) AS unit_price_filled_p90,
    ROUND(AVG(unit_price), 2) AS avg_unit_price,
    MIN(unit_price_filled) AS min_unit_price_filled,
    PERCENTILE_APPROX(unit_price, 0.9) AS unit_price_p90,
    CORR(unit_price, quantity) AS corr_unit_price_quantity,
    SUM(unit_price) AS total_unit_price,
    RANK() OVER (PARTITION BY CASE WHEN unit_price_filled >= 61 THEN 'PREMIUM' ELSE 'STANDARD' END ORDER BY PERCENTILE_APPROX(unit_price_filled, 0.9) DESC) AS unit_price_filled_p90_rank
FROM base
GROUP BY CASE WHEN unit_price_filled >= 61 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN quantity >= 9 THEN 'LARGE' ELSE 'SMALL' END
HAVING MIN(unit_price_filled) >= 0;
