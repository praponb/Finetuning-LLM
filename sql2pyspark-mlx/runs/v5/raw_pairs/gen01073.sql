WITH valid AS (
    -- Basic cleansing and defaults
    SELECT *, CASE WHEN dept_name IN ('Sales', 'Engineering') THEN 'Tier 1' ELSE 'Other' END AS dept_name_group
    FROM departments
    WHERE dept_name = 'Engineering'
),
with_last AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.salary
    FROM valid s
    LEFT JOIN LATERAL (
            SELECT c.salary
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MIN(salary) OVER w_ordered AS running_min_salary,
        AVG(salary) OVER w_ordered AS running_avg_salary,
        COUNT(salary) OVER w_ordered AS running_count_salary
    FROM with_last
    WHERE dept_name IN ('Finance', 'HR')
    WINDOW w_ordered AS (PARTITION BY location ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_latest AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.hire_date
    FROM enriched s
    JOIN LATERAL (
            SELECT c.hire_date
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
SELECT
    id,
    location,
    budget,
    dept_name_group,
    salary,
    running_avg_salary,
    running_count_salary,
    hire_date,
    CASE WHEN location RLIKE '^(London|Singapore)$' THEN 'EXTENDED' ELSE 'GROUP_A' END AS location_zone
FROM with_latest;
