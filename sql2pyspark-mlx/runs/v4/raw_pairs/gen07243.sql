SELECT * FROM (SELECT id, channel, amount, discount, shipping_fee FROM orders) src UNPIVOT (metric_value FOR kpi IN (amount, discount, shipping_fee));
