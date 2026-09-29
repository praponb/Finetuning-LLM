SELECT shipping_fee FROM orders EXCEPT SELECT unit_price FROM order_items ORDER BY shipping_fee DESC;
