WITH filtered_departments AS (
    SELECT *
    FROM departments
    WHERE budget BETWEEN 2908000 AND 3079000 AND budget < 2786000
),
DepartmentsStats AS (
    SELECT CASE WHEN budget >= 3611000 THEN 'high' WHEN budget >= 1979000 THEN 'mid' ELSE 'low' END AS budget_bucket, SORT_ARRAY(COLLECT_SET(dept_name)) AS dept_name_list
    FROM filtered_departments
    GROUP BY CASE WHEN budget >= 3611000 THEN 'high' WHEN budget >= 1979000 THEN 'mid' ELSE 'low' END
)
SELECT *
FROM DepartmentsStats
