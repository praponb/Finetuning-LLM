WITH cleaned AS (
    SELECT *, CASE WHEN units >= 40 THEN 'High' WHEN units >= 20 THEN 'Medium' ELSE 'Low' END units_level
    FROM store_sales
    WHERE revenue >= 2700
)
SELECT
    id,
    sale_date,
    units,
    NTILE(10) OVER (PARTITION BY store_id ORDER BY revenue, id) revenue_bucket,
    CUME_DIST() OVER (PARTITION BY product_id, store_id ORDER BY units DESC NULLS FIRST) units_cume_dist
FROM cleaned;
