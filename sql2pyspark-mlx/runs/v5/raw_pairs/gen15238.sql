SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY order_date DESC, id) AS row_num
FROM orders
QUALIFY row_num = 1;
