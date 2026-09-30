SELECT
    id,
    store_id,
    product_id,
    revenue,
    units - revenue AS revenue_per_unit
FROM store_sales
WHERE store_id IN (2, 4)
ORDER BY product_id DESC
