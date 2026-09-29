SELECT 'product' source, cost value FROM products WHERE brand IN ('Initech', 'Acme', 'Globex') UNION SELECT 'subscription' source, monthly_fee value FROM subscriptions ORDER BY source DESC
