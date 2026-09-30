SELECT p.cost, p.price, ss.product_id FROM products p INNER JOIN store_sales ss ON p.id = ss.product_id WHERE ss.revenue > 1300
