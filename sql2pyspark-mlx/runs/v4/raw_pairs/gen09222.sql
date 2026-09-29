SELECT
    products.launch_date,
    products.cost,
    store_sales.revenue,
    store_sales.units - store_sales.revenue AS revenue_per_unit
FROM products
LEFT JOIN store_sales ON store_sales.product_id = products.id
WHERE store_sales.store_id IN (2, 6) AND YEAR(store_sales.sale_date) >= 2024
