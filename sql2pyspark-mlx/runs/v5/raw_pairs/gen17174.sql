SELECT
    suppliers.id,
    suppliers.supplier_name,
    suppliers.country,
    suppliers.city
FROM suppliers
WHERE EXISTS (SELECT 1 FROM products WHERE products.supplier_id = suppliers.id AND LOWER(TRIM(products.product_name)) = 'water bottle')
ORDER BY country;
