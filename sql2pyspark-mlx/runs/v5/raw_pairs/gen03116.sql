SELECT x.carrier, y.status FROM (SELECT DISTINCT carrier FROM shipments) x CROSS JOIN (SELECT DISTINCT status FROM subscriptions) y ORDER BY carrier, status
