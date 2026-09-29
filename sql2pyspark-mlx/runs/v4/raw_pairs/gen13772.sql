SELECT
    id,
    product_id,
    sale_date,
    units,
    store_id,
    LEAD(units, 1) OVER (PARTITION BY product_id ORDER BY sale_date, id) AS next_units,
    ROUND(units / SUM(units) OVER (PARTITION BY store_id, product_id), 3) AS units_share
FROM store_sales;
