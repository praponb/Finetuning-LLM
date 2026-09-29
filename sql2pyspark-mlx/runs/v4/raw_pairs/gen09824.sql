WITH filtered AS (
    SELECT
        *,
        LENGTH(dept_name) AS dept_name_length,
        COALESCE(dept_name, 'DIRECT') AS dept_name_filled
    FROM departments
    WHERE location = 'Chiang Mai'
)
SELECT
    CASE WHEN budget >= 3159000 THEN 'LARGE' ELSE 'SMALL' END AS budget_tier,
    dept_name,
    SIZE(COLLECT_SET(dept_name)) AS num_dept_names,
    COUNT(CASE WHEN dept_name = 'Marketing' THEN 1 END) AS marketing_count,
    PERCENTILE_APPROX(dept_name_length, ARRAY(0.25, 0.5, 0.75)) AS dept_name_length_quartiles
FROM filtered
GROUP BY CASE WHEN budget >= 3159000 THEN 'LARGE' ELSE 'SMALL' END, dept_name
HAVING SIZE(COLLECT_SET(dept_name)) < 1;
