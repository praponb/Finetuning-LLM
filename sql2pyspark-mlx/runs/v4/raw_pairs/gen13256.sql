SELECT id, launch_date, price, CASE WHEN price >= 1300 THEN 'High' WHEN price >= 450 THEN 'Medium' ELSE 'Low' END AS price_level FROM products
