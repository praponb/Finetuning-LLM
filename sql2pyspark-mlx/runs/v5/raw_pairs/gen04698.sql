SELECT
    i.id,
    i.quantity,
    i.store_id,
    i.product_id,
    ss.sale_date,
    ss.product_id store_sale_product_id,
    ss.units
FROM inventory i
INNER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
