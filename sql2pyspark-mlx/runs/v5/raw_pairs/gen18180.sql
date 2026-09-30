SELECT *
FROM (
    SELECT CASE WHEN budget >= 4221000 THEN 'high' WHEN budget >= 1603000 THEN 'mid' ELSE 'low' END AS budget_bucket, ROUND((100 * SUM(CASE WHEN dept_name = 'Marketing' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_marketing, SORT_ARRAY(COLLECT_SET(dept_name)) AS dept_name_list
    FROM departments
    WHERE budget BETWEEN 1493000 AND 3515000
    GROUP BY CASE WHEN budget >= 4221000 THEN 'high' WHEN budget >= 1603000 THEN 'mid' ELSE 'low' END
) AS x
WHERE pct_marketing >= 63;
