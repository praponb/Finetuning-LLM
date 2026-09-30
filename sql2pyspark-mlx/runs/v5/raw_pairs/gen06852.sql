SELECT cost, launch_date, GREATEST(price, stock_qty) AS greatest_price_stock_qty FROM products WHERE product_name LIKE 'D%';
