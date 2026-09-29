SELECT
    id,
    product_id,
    store_id,
    sale_date,
    units,
    SUM(units) OVER (PARTITION BY product_id, store_id ORDER BY sale_date, id) running_units
FROM store_sales
