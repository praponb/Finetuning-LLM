SELECT cost, tags, supplier_id, LOWER(brand) AS brand_lower FROM products WHERE supplier_id IN (11, 17, 35) AND price >= 1000 ORDER BY brand_lower DESC, 3 DESC;
