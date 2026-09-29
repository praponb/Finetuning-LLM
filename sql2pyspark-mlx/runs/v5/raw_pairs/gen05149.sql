SELECT id, cost, CONCAT_WS(',', brand, product_name) brand_product_name_key FROM products WHERE product_name LIKE '%Water%' AND (brand != 'Umbrella' OR supplier_id IS NOT NULL)
