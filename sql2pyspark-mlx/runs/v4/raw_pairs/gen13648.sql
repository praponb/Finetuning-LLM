SELECT *, CEIL(unit_price) AS unit_price_whole FROM order_items WHERE quantity BETWEEN 5 AND 8;
