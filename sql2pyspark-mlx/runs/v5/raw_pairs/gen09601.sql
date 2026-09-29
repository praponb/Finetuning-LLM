SELECT unit_price FROM order_items EXCEPT SELECT amount FROM transactions WHERE is_flagged
