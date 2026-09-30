SELECT id, plan, end_date FROM subscriptions WHERE monthly_fee >= 120 UNION ALL SELECT id, plan, end_date FROM subscriptions WHERE end_date <= '2024-03-01' ORDER BY id DESC
