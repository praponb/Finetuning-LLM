SELECT
    location,
    CASE WHEN dept_name IN ('Engineering', 'Marketing') THEN 'group_a' ELSE 'other' END AS dept_name_group,
    COUNT(*) AS total_count,
    SORT_ARRAY(COLLECT_SET(location)) AS location_list
FROM departments
WHERE budget > 3391000 OR budget BETWEEN 1258000 AND 3893000
GROUP BY 1, 2
ORDER BY location, dept_name_group
LIMIT 50
