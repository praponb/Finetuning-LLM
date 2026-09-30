SELECT cost, price, launch_date, ARRAY_JOIN(tags, '|') AS tags_text FROM products
