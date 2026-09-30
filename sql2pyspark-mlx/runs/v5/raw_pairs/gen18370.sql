WITH matched_keys AS (
    SELECT dept_name, id
    FROM departments
    INTERSECT
    SELECT dept_name, id
    FROM departments
    WHERE location <> 'Berlin'
),
reconciled AS (
    SELECT s.*
    FROM departments s
    INNER JOIN matched_keys k ON s.dept_name = k.dept_name AND s.id = k.id
)
SELECT
    id,
    dept_name,
    location,
    budget,
    CASE WHEN dept_name RLIKE '^(HR|Finance)$' THEN 'GROUP_A' WHEN dept_name RLIKE '^(Engineering|Sales)$' THEN 'PRIORITY' ELSE 'CORE' END AS dept_name_zone
FROM reconciled
ORDER BY dept_name;
