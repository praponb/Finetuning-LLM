SELECT x.account_type, y.plan FROM (SELECT DISTINCT account_type FROM accounts) x CROSS JOIN (SELECT DISTINCT plan FROM subscriptions) y ORDER BY account_type, plan;
