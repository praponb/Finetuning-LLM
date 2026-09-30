SELECT id, customer_id, kpi, value FROM (SELECT id, customer_id, amount, discount, shipping_fee FROM orders) src UNPIVOT (value FOR kpi IN (amount, discount, shipping_fee));
