WITH valid_departments AS (
    SELECT *, LOWER(location) AS location_lower
    FROM departments
    WHERE dept_name IN ('HR', 'Finance', 'Legal')
),
summary AS (
    SELECT CASE WHEN budget >= 4359000 THEN 'high' WHEN budget >= 1491000 THEN 'mid' ELSE 'low' END AS budget_bucket, SORT_ARRAY(COLLECT_SET(dept_name)) AS dept_name_list, MIN(budget) AS min_budget
    FROM valid_departments
    GROUP BY CASE WHEN budget >= 4359000 THEN 'high' WHEN budget >= 1491000 THEN 'mid' ELSE 'low' END
)
SELECT *
FROM summary;
