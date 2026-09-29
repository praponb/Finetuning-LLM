SELECT unit_price AS value FROM order_items WHERE product_id IS NOT NULL EXCEPT SELECT cost AS value FROM shipments;
