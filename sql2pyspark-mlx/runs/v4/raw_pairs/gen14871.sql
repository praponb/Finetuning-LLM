SELECT status, SUM(monthly_fee) AS total_monthly_fee, RANK() OVER (ORDER BY SUM(monthly_fee) DESC) AS rnk FROM subscriptions WHERE start_date BETWEEN '2024-06-01' AND '2025-08-04' GROUP BY status;
