SELECT
    store_sales.id,
    store_sales.units,
    store_sales.product_id,
    products.price,
    products.product_name,
    stores.opened_date,
    stores.store_name,
    DATE_TRUNC('year', store_sales.sale_date) AS sale_date_period
FROM store_sales
LEFT OUTER JOIN products ON store_sales.product_id = products.id
INNER JOIN stores ON stores.id = store_sales.store_id
WHERE store_sales.sale_date BETWEEN '2025-04-02' AND '2025-07-01';
