SELECT CASE WHEN budget >= 3164000 THEN 'high' WHEN budget >= 1808000 THEN 'mid' ELSE 'low' END AS budget_bucket, SUM(budget) AS total_budget FROM departments GROUP BY 1 HAVING SUM(budget) < 20598000
