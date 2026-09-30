SELECT x.channel, y.status FROM (SELECT DISTINCT channel FROM orders) x CROSS JOIN (SELECT DISTINCT status FROM accounts) y ORDER BY channel, status
