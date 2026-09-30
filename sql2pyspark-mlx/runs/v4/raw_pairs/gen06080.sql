SELECT YEAR(end_date) AS end_year, SUM(CASE WHEN plan = 'standard' THEN monthly_fee ELSE 0 END) AS standard_monthly_fee, COUNT(monthly_fee) AS monthly_fee_count
FROM subscriptions
GROUP BY YEAR(end_date);
