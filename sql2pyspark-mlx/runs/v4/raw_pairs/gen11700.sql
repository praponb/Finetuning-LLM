SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date DESC, id) AS pos FROM orders) AS ranked WHERE pos <= 5
