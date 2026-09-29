SELECT DISTINCT plan, status FROM subscriptions WHERE monthly_fee BETWEEN 120 AND 150 ORDER BY plan, status LIMIT 5;
