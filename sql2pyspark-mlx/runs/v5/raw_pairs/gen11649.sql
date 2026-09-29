SELECT CASE WHEN budget >= 4062000 THEN 'high' WHEN budget >= 1086000 THEN 'mid' ELSE 'low' END AS budget_bucket, location, ROUND(STDDEV(budget), 2) AS stddev_budget FROM departments GROUP BY 1, 2;
