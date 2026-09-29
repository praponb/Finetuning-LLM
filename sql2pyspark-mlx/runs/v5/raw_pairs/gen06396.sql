SELECT id, customer_id, measure, value FROM (SELECT id, customer_id, shipping_fee, discount, amount FROM orders) src UNPIVOT (value FOR measure IN (shipping_fee, discount, amount));
