SELECT
    id,
    product_id,
    units,
    ROUND(units / SUM(units) OVER (PARTITION BY product_id), 3) units_share
FROM store_sales
WHERE product_id = 4
