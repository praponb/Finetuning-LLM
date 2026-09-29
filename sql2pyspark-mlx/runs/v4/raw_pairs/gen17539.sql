SELECT
    hire_cohort,
    is_remote,
    last_hire_date,
    l2_count
FROM (
    SELECT
        CASE WHEN YEAR(hire_date) < 2024 THEN 'before_2024' WHEN YEAR(hire_date) = 2024 THEN '2024' ELSE 'after_2024' END AS hire_cohort,
        is_remote,
        MAX(hire_date) AS last_hire_date,
        COUNT(CASE WHEN level = 'L2' THEN 1 END) AS l2_count
    FROM employees
    GROUP BY CASE WHEN YEAR(hire_date) < 2024 THEN 'before_2024' WHEN YEAR(hire_date) = 2024 THEN '2024' ELSE 'after_2024' END, is_remote
) AS s
WHERE l2_count >= 6
ORDER BY l2_count DESC
