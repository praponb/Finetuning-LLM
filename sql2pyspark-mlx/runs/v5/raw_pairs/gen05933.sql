SELECT
    id,
    product_id,
    sale_date,
    units,
    LAG(units, 1) OVER (PARTITION BY product_id ORDER BY sale_date, id) prev_units
FROM store_sales;
