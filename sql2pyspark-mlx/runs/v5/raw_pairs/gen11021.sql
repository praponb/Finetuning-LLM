SELECT products.category, MIN(store_sales.sale_date) AS min_sale_date, MAX(store_sales.sale_date) AS latest_sale_date
FROM store_sales
INNER JOIN products ON store_sales.product_id = products.id
INNER JOIN stores ON store_sales.store_id = stores.id
WHERE products.launch_date >= '2025-06-01'
GROUP BY products.category
