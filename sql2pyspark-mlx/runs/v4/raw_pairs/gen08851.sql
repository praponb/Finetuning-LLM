SELECT
    id,
    product_id,
    sale_date,
    units,
    LEAD(units, 1) OVER (PARTITION BY product_id ORDER BY sale_date, id) AS next_units
FROM store_sales
WHERE units BETWEEN 17 AND 39;
