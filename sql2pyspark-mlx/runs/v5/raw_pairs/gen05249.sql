SELECT status, SUM(monthly_fee) AS total_monthly_fee, DENSE_RANK() OVER (ORDER BY SUM(monthly_fee) DESC) AS rnk
FROM subscriptions
GROUP BY status
ORDER BY status ASC;
