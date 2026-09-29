SELECT id, store_id, LEAST(revenue, units) AS least_revenue_units
FROM store_sales
WHERE units > 21 AND product_id IN (2, 4, 6, 8) AND sale_date > '2024-06-19';
