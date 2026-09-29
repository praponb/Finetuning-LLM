SELECT status, SUM(CASE WHEN status = 'EXPIRED' THEN monthly_fee ELSE 0 END) AS expired_monthly_fee
FROM subscriptions
GROUP BY status
ORDER BY expired_monthly_fee;
