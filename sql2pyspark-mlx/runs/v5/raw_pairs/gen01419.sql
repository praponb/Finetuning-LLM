SELECT id, customer_id, order_date FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date, id) AS row_num FROM orders) AS r WHERE row_num = 1 ORDER BY order_date ASC;
