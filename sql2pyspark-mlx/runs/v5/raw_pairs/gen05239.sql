SELECT id, brand, price, EXPLODE(tags) AS tag FROM products WHERE stock_qty IS NULL;
