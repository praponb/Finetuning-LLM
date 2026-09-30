SELECT x.account_type, y.status FROM (SELECT DISTINCT account_type FROM accounts) x CROSS JOIN (SELECT DISTINCT status FROM shipments) y ORDER BY account_type, status
