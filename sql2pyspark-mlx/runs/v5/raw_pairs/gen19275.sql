SELECT id, status, CASE WHEN monthly_fee >= 204 THEN 'High' WHEN monthly_fee >= 122 THEN 'Medium' ELSE 'Low' END AS monthly_fee_level
FROM subscriptions
WHERE monthly_fee <= 160 AND plan IN ('basic', 'enterprise', 'standard')
ORDER BY id DESC, monthly_fee_level;
