SELECT
    store_sales.id,
    store_sales.sale_date,
    products.cost,
    products.stock_qty,
    suppliers.city,
    suppliers.rating
FROM store_sales
INNER JOIN products ON products.id = store_sales.product_id
RIGHT JOIN suppliers ON suppliers.id = products.supplier_id
WHERE store_sales.revenue > 3500 OR suppliers.supplier_name IS NOT NULL
