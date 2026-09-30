SELECT a.channel, b.method FROM (SELECT DISTINCT channel FROM orders) a CROSS JOIN (SELECT DISTINCT method FROM payments) b ORDER BY channel, method;
