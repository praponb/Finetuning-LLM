SELECT x.account_type, y.carrier FROM (SELECT DISTINCT account_type FROM accounts) x CROSS JOIN (SELECT DISTINCT carrier FROM shipments) y ORDER BY account_type, carrier
