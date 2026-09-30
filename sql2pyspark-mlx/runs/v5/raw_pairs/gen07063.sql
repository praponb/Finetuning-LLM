SELECT id, tags, CONCAT_WS('-', product_name, brand) AS product_name_brand_key FROM products WHERE price IS NOT NULL AND cost > 450
