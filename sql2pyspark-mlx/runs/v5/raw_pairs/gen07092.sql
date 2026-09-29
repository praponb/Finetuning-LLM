WITH filtered_departments AS (
    SELECT *, INITCAP(TRIM(location)) AS location_title
    FROM departments
    WHERE budget BETWEEN 2810000 AND 3220000 AND location IN ('Singapore', 'Tokyo', 'Bangkok')
),
dept_name_summary AS (
    SELECT
        dept_name,
        CASE WHEN location IN ('Singapore', 'New York') THEN 'priority' ELSE 'other' END AS location_group,
        SUM(CASE WHEN location = 'Chiang Mai' THEN 1 ELSE 0 END) AS chiang_mai_count,
        AVG(budget) AS avg_budget
    FROM filtered_departments
    GROUP BY dept_name, CASE WHEN location IN ('Singapore', 'New York') THEN 'priority' ELSE 'other' END
)
SELECT dept_name, location_group, avg_budget
FROM dept_name_summary
ORDER BY location_group, dept_name;
