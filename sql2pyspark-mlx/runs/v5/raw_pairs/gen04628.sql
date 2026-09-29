SELECT id, category FROM products WHERE price BETWEEN 450 AND 520 EXCEPT SELECT id, category FROM products WHERE cost <= 370;
