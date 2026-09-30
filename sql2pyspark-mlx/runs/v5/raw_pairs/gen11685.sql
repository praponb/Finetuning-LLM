WITH kept_keys AS (
    SELECT store_id, product_id
    FROM store_sales
    INTERSECT
    SELECT store_id, product_id
    FROM store_sales
    WHERE units IS NOT NULL
),
verified AS (
    SELECT s.*
    FROM store_sales s
    INNER JOIN kept_keys k ON s.store_id = k.store_id AND s.product_id = k.product_id
)
SELECT
    DATE_TRUNC('WEEK', sale_date) AS week_start,
    CASE WHEN units >= 32 THEN 'LARGE' ELSE 'SMALL' END AS units_tier,
    COUNT(DISTINCT store_id) AS distinct_store_ids,
    COUNT(*) AS cnt,
    CORR(units, revenue) AS corr_units_revenue,
    PERCENTILE_APPROX(units, ARRAY(0.25, 0.5, 0.75)) AS units_quartiles,
    MIN(units) AS min_units
FROM verified
GROUP BY DATE_TRUNC('WEEK', sale_date), CASE WHEN units >= 32 THEN 'LARGE' ELSE 'SMALL' END
