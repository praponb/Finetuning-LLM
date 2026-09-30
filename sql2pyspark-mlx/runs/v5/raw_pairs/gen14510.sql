SELECT id, sale_date, ROUND(units / revenue, 2) AS revenue_per_unit
FROM store_sales
WHERE product_id = 10
ORDER BY revenue_per_unit DESC, sale_date ASC;
