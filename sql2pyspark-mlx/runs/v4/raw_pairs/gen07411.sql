SELECT CASE WHEN balance >= 82000 THEN 'high' WHEN balance >= 9000 THEN 'mid' ELSE 'low' END balance_bucket, status, MIN(opened_date) first_opened_date
FROM accounts
GROUP BY CASE WHEN balance >= 82000 THEN 'high' WHEN balance >= 9000 THEN 'mid' ELSE 'low' END, status
