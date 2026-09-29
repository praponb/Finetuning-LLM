SELECT *, RANK() OVER (PARTITION BY store_id ORDER BY order_date DESC) AS row_num FROM orders QUALIFY row_num = 1;
