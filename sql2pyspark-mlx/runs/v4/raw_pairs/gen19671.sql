SELECT amount, 'transaction' AS source FROM transactions UNION SELECT unit_price, 'order_item' AS source FROM order_items ORDER BY source
