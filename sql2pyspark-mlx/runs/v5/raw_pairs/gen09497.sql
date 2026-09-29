SELECT YEAR(end_date) end_year, COUNT(monthly_fee) monthly_fee_count, COUNT(*) total_count FROM subscriptions GROUP BY YEAR(end_date) HAVING COUNT(*) >= 3
