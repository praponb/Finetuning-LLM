SELECT a.status, b.method FROM (SELECT DISTINCT status FROM accounts) a CROSS JOIN (SELECT DISTINCT method FROM payments) b ORDER BY status, method
