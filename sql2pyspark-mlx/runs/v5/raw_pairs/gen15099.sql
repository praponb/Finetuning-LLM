SELECT
    id,
    product_id,
    store_id,
    revenue + units AS revenue_per_unit
FROM store_sales
WHERE sale_date <= '2023-11-01';
