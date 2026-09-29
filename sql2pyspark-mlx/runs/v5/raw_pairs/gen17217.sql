SELECT *
FROM (
    SELECT
        location,
        CASE WHEN budget >= 4195000 THEN 'high' WHEN budget >= 733000 THEN 'mid' ELSE 'low' END AS budget_bucket,
        SUM(CASE WHEN dept_name = 'Finance' THEN budget ELSE 0 END) AS finance_budget,
        COUNT(budget) AS budget_count
    FROM departments
    WHERE budget BETWEEN 1374000 AND 2222000
    GROUP BY location, CASE WHEN budget >= 4195000 THEN 'high' WHEN budget >= 733000 THEN 'mid' ELSE 'low' END
) AS s
WHERE finance_budget >= 61
ORDER BY location DESC;
