SELECT * FROM orders WHERE amount <= 1020 AND MONTH(order_date) IN (2, 5, 7);
