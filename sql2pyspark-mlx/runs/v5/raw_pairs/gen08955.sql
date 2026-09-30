SELECT * FROM (SELECT CASE WHEN credit_limit >= 13000 THEN 'high' WHEN credit_limit >= 3900 THEN 'mid' ELSE 'low' END AS credit_limit_bucket, segment, credit_limit FROM customers)
PIVOT (SUM(credit_limit) FOR segment IN ('Corporate', 'Consumer'));
