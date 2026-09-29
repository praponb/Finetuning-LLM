SELECT
    *,
    LAG(revenue, 2) OVER (PARTITION BY product_id ORDER BY units, id) AS prev_revenue,
    ROUND(units / SUM(units) OVER (PARTITION BY product_id, store_id), 4) AS units_share
FROM store_sales;
