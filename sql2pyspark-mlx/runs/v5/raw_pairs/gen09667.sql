SELECT
    products.brand,
    products.category,
    products.stock_qty,
    suppliers.city
FROM products
LEFT JOIN suppliers ON products.supplier_id = suppliers.id
WHERE suppliers.country != 'JP' AND products.price < 490
