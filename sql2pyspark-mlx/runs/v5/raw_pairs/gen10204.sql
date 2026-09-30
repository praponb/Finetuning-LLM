WITH base AS (
    SELECT *, CASE WHEN dept_name IN ('Finance', 'Marketing') THEN 'Group A' ELSE 'Other' END dept_name_group
    FROM departments
    WHERE dept_name IN ('Finance', 'HR', 'Engineering') AND dept_name IS NOT NULL
),
agg AS (
    SELECT CASE WHEN budget >= 3919000 THEN 'high' WHEN budget >= 914000 THEN 'mid' ELSE 'low' END budget_bucket, dept_name, SORT_ARRAY(COLLECT_SET(dept_name)) dept_name_list
    FROM base
    GROUP BY CASE WHEN budget >= 3919000 THEN 'high' WHEN budget >= 914000 THEN 'mid' ELSE 'low' END, dept_name
)
SELECT budget_bucket, dept_name, dept_name_list
FROM agg
ORDER BY 1;
