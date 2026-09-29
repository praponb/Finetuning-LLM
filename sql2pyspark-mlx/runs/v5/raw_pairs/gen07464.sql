SELECT * FROM (SELECT status, channel, discount FROM orders) src PIVOT (SUM(discount) FOR channel IN ('web', 'mobile', 'partner'));
