SELECT
    units,
    revenue,
    product_id,
    units + revenue AS revenue_per_unit
FROM store_sales
ORDER BY product_id, revenue_per_unit DESC
