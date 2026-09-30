WITH valid_employees AS (
    SELECT *, REGEXP_REPLACE(full_name, ' ', '-') full_name_slug
    FROM employees
    WHERE salary BETWEEN 97000 AND 101000
),
Agg AS (
    SELECT
        job_title,
        CASE WHEN level IN ('L2', 'L3') THEN 'tier_1' ELSE 'other' END level_group,
        MAX(salary) - MIN(salary) salary_range,
        CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(job_title))) job_title_values
    FROM valid_employees
    GROUP BY job_title, CASE WHEN level IN ('L2', 'L3') THEN 'tier_1' ELSE 'other' END
)
SELECT *
FROM Agg
ORDER BY job_title
