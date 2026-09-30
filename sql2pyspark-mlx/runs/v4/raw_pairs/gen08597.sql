WITH latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY last_restock DESC NULLS LAST, id) AS row_num
        FROM inventory
    ) ranked
    WHERE row_num = 1
)
-- Aggregate with conditional metrics
SELECT
    store_id,
    CASE WHEN quantity >= 235 THEN 'HIGH' ELSE 'LOW' END AS quantity_tier,
    DATE_TRUNC('YEAR', last_restock) AS year_start,
    AVG(quantity) AS avg_quantity,
    COUNT(*) AS cnt,
    PERCENTILE_APPROX(quantity, ARRAY(0.25, 0.5, 0.75)) AS quantity_quartiles,
    PERCENTILE_APPROX(quantity, ARRAY(0.25, 0.5, 0.75)) AS quantity_quartiles_2,
    PERCENTILE_APPROX(quantity, 0.9) AS quantity_p90,
    PERCENT_RANK() OVER (PARTITION BY store_id ORDER BY PERCENTILE_APPROX(quantity, 0.9) DESC) AS quantity_p90_percent_rank
FROM latest
GROUP BY store_id, CASE WHEN quantity >= 235 THEN 'HIGH' ELSE 'LOW' END, DATE_TRUNC('YEAR', last_restock)
HAVING AVG(quantity) >= 0;
