SELECT
    id,
    sale_date,
    units,
    units + revenue AS revenue_per_unit
FROM store_sales
WHERE store_id = 5;
