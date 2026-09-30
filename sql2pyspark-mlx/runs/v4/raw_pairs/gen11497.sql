SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date, id) AS row_num FROM orders) AS ranked WHERE row_num <= 3
