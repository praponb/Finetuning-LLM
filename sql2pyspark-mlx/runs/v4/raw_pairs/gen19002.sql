SELECT
    id,
    store_id,
    sale_date,
    units,
    LEAD(units, 1) OVER (PARTITION BY store_id ORDER BY sale_date, id) AS next_units
FROM store_sales
WHERE sale_date < '2024-04-01';
