WITH src AS (
    SELECT *
    FROM employees
    WHERE level IN ('L4', 'L2', 'L1') AND salary < 100000 AND dept_id IN (23, 24)
),
agg AS (
    SELECT CASE WHEN salary >= 126000 THEN 'high' WHEN salary >= 56000 THEN 'mid' ELSE 'low' END AS salary_bucket, SUM(CASE WHEN level = 'L5' THEN 1 ELSE 0 END) AS l5_count, SORT_ARRAY(COLLECT_SET(job_title)) AS job_title_list
    FROM src
    GROUP BY CASE WHEN salary >= 126000 THEN 'high' WHEN salary >= 56000 THEN 'mid' ELSE 'low' END
)
SELECT salary_bucket, job_title_list
FROM agg
ORDER BY salary_bucket;
