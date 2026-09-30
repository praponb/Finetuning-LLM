SELECT start_date, CASE WHEN status = 'PAUSED' THEN 1 ELSE 0 END AS is_paused
FROM subscriptions
WHERE customer_id = 29
ORDER BY is_paused DESC;
