SELECT id, brand, launch_date FROM products WHERE price >= 340 INTERSECT SELECT id, brand, launch_date FROM products WHERE price <= 1060
