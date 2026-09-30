SELECT a.status, b.plan FROM (SELECT DISTINCT status FROM orders) a CROSS JOIN (SELECT DISTINCT plan FROM subscriptions) b ORDER BY status, plan
