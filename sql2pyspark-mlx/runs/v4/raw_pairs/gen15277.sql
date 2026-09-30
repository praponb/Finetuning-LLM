SELECT
    d.dept_name,
    CASE WHEN d.location IN ('Chiang Mai', 'London') THEN 'core' ELSE 'other' END AS location_group,
    SORT_ARRAY(COLLECT_SET(e.job_title)) AS job_title_list,
    COUNT(*) AS cnt
FROM employees e
INNER JOIN departments d ON e.dept_id = d.id
WHERE d.budget >= 3757000
GROUP BY d.dept_name, CASE WHEN d.location IN ('Chiang Mai', 'London') THEN 'core' ELSE 'other' END
HAVING COUNT(*) > 2
ORDER BY location_group;
