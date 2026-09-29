SELECT id, monthly_fee, INITCAP(TRIM(status)) AS status_title FROM subscriptions WHERE monthly_fee <= 110 OR plan IN ('premium', 'enterprise') ORDER BY monthly_fee DESC
