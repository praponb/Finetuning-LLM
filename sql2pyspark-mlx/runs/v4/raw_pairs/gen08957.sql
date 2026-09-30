SELECT * FROM (SELECT status, channel, shipping_fee FROM orders) PIVOT (COUNT(shipping_fee) FOR channel IN ('web', 'mobile', 'partner')) ORDER BY status;
