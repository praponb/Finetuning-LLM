WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        full_name,
        dept_id,
        manager_id,
        salary,
        level,
        ROUND(salary * 1.15, 2) AS salary_scaled
    FROM employees
    WHERE salary > 84000
),
matched_keys AS (
    -- Set difference on keys
    SELECT manager_id, id
    FROM valid
    EXCEPT DISTINCT
    SELECT manager_id, id
    FROM employees
    WHERE salary < 123000
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM valid s
    INNER JOIN matched_keys k ON s.manager_id = k.manager_id AND s.id = k.id
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        dept_id,
        level,
        salary_scaled,
        MIN(salary_scaled) OVER (PARTITION BY level) AS level_min_salary_scaled,
        ROUND(salary / SUM(salary) OVER (PARTITION BY level, manager_id), 4) AS salary_share,
        RANK() OVER (PARTITION BY level ORDER BY salary DESC) AS salary_rank,
        ROUND(salary / SUM(salary) OVER (PARTITION BY level), 4) AS salary_share_2,
        CUME_DIST() OVER (ORDER BY salary_scaled DESC NULLS FIRST) AS salary_scaled_cume_dist
    FROM kept_rows
),
valid_employees AS (
    -- Keep recent, valid rows
    SELECT
        *,
        GREATEST(salary_rank, salary_share_2) AS greatest_salary_rank_salary_share_2,
        CASE WHEN salary_scaled >= 84 THEN 'High' WHEN salary_scaled >= 28 THEN 'Medium' ELSE 'Low' END AS salary_scaled_level
    FROM with_windows
    WHERE level IS NOT NULL
)
SELECT
    s.id,
    s.dept_id,
    s.level,
    s.salary_scaled,
    s.salary_share,
    s.salary_rank,
    s.salary_share_2,
    s.salary_scaled_cume_dist,
    s.salary_scaled_level,
    CASE WHEN level RLIKE '^(L1|L4)$' THEN 'GROUP_B' WHEN level RLIKE '^(L2|L3)$' THEN 'EXTENDED' ELSE 'CORE' END AS level_zone,
    (SELECT MIN(x.salary) FROM employees x WHERE x.dept_id = s.dept_id) AS min_employee_salary
FROM valid_employees s;
