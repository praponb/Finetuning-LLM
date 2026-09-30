WITH base AS (
    SELECT *
    FROM departments
    WHERE budget <= 1845000
),
departments_stats AS (
    SELECT CASE WHEN budget >= 3226000 THEN 'high' WHEN budget >= 1986000 THEN 'mid' ELSE 'low' END AS budget_bucket, COUNT(*) AS department_count, SORT_ARRAY(COLLECT_SET(dept_name)) AS dept_name_list
    FROM base
    GROUP BY CASE WHEN budget >= 3226000 THEN 'high' WHEN budget >= 1986000 THEN 'mid' ELSE 'low' END
)
SELECT budget_bucket, department_count
FROM departments_stats
WHERE department_count < 7
ORDER BY 1
