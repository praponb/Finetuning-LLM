WITH metrics_long AS (
    SELECT
        id,
        brand,
        measure,
        value
    FROM (
        SELECT
            id,
            brand,
            cost,
            price
        FROM products
    ) src
    UNPIVOT (
        value FOR measure IN (cost AS COST_VAL, price AS PRICE)
    )
)
SELECT
    CASE WHEN value >= 70 THEN 'HIGH' ELSE 'LOW' END AS value_tier,
    brand,
    APPROX_PERCENTILE(value, ARRAY(0.25, 0.5, 0.75)) AS value_quartiles,
    APPROX_PERCENTILE(value, ARRAY(0.25, 0.5, 0.75)) AS value_quartiles_2,
    PERCENTILE_APPROX(value, 0.9) AS value_p90,
    ROUND(SUM(value) / NULLIF(COUNT(CASE WHEN brand = 'Stark' THEN 1 END), 0), 2) AS value_per_stark,
    AVG(value) FILTER (WHERE measure = 'COST_VAL') AS cost_val_avg_value,
    ROUND(SUM(value) / NULLIF(COUNT(CASE WHEN brand = 'Initech' THEN 1 END), 0), 2) AS value_per_initech
FROM metrics_long
GROUP BY CUBE (CASE WHEN value >= 70 THEN 'HIGH' ELSE 'LOW' END, brand)
ORDER BY value_p90 ASC;
