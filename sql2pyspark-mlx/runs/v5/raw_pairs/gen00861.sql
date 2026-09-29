SELECT x.status, y.method FROM (SELECT DISTINCT status FROM accounts) x CROSS JOIN (SELECT DISTINCT method FROM payments) y;
