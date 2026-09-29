SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id, order_id ORDER BY quantity NULLS FIRST, id) AS row_num
FROM order_items
QUALIFY row_num = 1;
