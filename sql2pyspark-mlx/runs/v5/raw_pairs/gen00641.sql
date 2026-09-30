SELECT product_id, sale_date, COALESCE(units, 0) AS units_filled
FROM store_sales
WHERE sale_date > '2023-04-01'
