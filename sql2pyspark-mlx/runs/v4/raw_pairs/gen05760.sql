SELECT * FROM (SELECT plan, AVG(monthly_fee) AS avg_monthly_fee FROM subscriptions WHERE end_date < '2024-08-01' GROUP BY plan) AS t WHERE avg_monthly_fee > 80 ORDER BY plan DESC;
