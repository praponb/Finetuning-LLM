SELECT a.channel, b.carrier FROM (SELECT DISTINCT channel FROM orders) a CROSS JOIN (SELECT DISTINCT carrier FROM shipments) b ORDER BY channel, carrier;
