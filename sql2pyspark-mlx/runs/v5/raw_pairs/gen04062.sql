SELECT id, price, product_name, category FROM products WHERE price < (SELECT MIN(price) FROM products);
