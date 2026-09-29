SELECT id, brand
FROM products
WHERE (supplier_id = 37 AND brand <> 'Acme') OR ARRAY_CONTAINS(tags, 'bestseller')
