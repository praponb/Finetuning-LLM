WITH ranked_order_items AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity DESC, id) AS row_num FROM order_items) SELECT * FROM ranked_order_items WHERE row_num <= 2;
