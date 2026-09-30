SELECT
    id,
    category,
    brand,
    cost,
    launch_date,
    supplier_id,
    AVG(price) OVER w_hist AS price_avg_3,
    SUM(price) OVER w_grp AS running_price,
    NTILE(10) OVER w_part AS cost_bucket
FROM products
WINDOW w_hist AS (PARTITION BY brand, category ORDER BY cost, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), w_grp AS (PARTITION BY brand ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_part AS (PARTITION BY category, supplier_id ORDER BY cost, id)
