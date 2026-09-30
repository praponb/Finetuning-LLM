SELECT plan, SUM(CASE WHEN status = 'PAUSED' THEN monthly_fee ELSE 0 END) paused_monthly_fee, MAX(end_date) latest_end_date
FROM subscriptions
WHERE MONTH(end_date) IN (1, 4, 7) AND monthly_fee <= 200
GROUP BY plan
HAVING paused_monthly_fee < 51
ORDER BY latest_end_date ASC, plan ASC
LIMIT 20;
