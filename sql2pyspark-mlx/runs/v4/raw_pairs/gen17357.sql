SELECT *, GREATEST(quantity, unit_price) greatest_quantity_unit_price, FLOOR(quantity) quantity_whole FROM order_items WHERE product_id IS NOT NULL;
