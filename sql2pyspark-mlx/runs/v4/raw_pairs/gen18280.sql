SELECT *, CASE WHEN balance >= 61000 THEN 'High' WHEN balance >= 33000 THEN 'Medium' ELSE 'Low' END AS balance_level FROM accounts ORDER BY balance DESC
