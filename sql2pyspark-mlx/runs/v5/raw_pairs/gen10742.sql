SELECT product_id, store_id, ROUND(units / revenue, 2) AS revenue_per_unit
FROM store_sales
WHERE NOT (revenue BETWEEN 1700 AND 3800 AND units >= 26 AND sale_date BETWEEN '2023-11-01' AND '2024-10-01')
ORDER BY product_id DESC
