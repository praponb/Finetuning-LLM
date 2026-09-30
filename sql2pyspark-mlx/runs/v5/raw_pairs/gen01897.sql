SELECT id, order_date, discount, LEAD(discount, 2) OVER (ORDER BY order_date, id) AS next_discount FROM orders WHERE discount < 0.23 ORDER BY order_date;
