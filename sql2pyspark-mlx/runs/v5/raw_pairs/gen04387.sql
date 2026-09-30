SELECT
    store_sales.sale_date,
    products.brand,
    products.category,
    stores.opened_date,
    stores.store_name
FROM store_sales
INNER JOIN products ON store_sales.product_id = products.id
INNER JOIN stores ON store_sales.store_id = stores.id
ORDER BY store_name DESC
