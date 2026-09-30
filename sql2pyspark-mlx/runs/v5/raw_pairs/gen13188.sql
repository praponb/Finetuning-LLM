WITH deduped AS (
    SELECT
        id,
        product_name,
        brand,
        price,
        cost,
        launch_date,
        tags
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY supplier_id, category ORDER BY stock_qty DESC NULLS LAST, id) AS rn
        FROM products
    ) t
    WHERE rn = 1
)
SELECT
    DATE_TRUNC('WEEK', launch_date) AS week_start,
    DATE_TRUNC('YEAR', launch_date) AS year_start,
    MAX(price) AS max_price,
    SUM(CASE WHEN brand = 'Initech' THEN price ELSE 0 END) AS initech_price,
    ROUND(AVG(cost), 2) AS avg_cost,
    ROUND(AVG(price), 2) AS avg_price,
    APPROX_PERCENTILE(price, 0.95) AS price_p95
FROM deduped
GROUP BY 1, 2
ORDER BY week_start DESC NULLS LAST;
