SELECT shipping_fee AS value FROM orders EXCEPT SELECT price AS value FROM products WHERE stock_qty < 191;
