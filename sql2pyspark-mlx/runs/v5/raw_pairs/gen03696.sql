SELECT
    id,
    product_id,
    revenue,
    ROUND(revenue / units, 2) AS revenue_per_unit
FROM store_sales
