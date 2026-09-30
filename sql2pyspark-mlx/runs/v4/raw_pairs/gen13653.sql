WITH filtered AS (
    -- Keep recent, valid rows
    SELECT *, COALESCE(location, 'N/A') AS location_filled
    FROM departments
    WHERE dept_name IS NOT NULL
),
latest_match AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.level,
        latest.employee_salary
    FROM filtered s
    LEFT JOIN LATERAL (
            SELECT c.level, c.salary AS employee_salary
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY level, dept_name ORDER BY employee_salary DESC, id) AS latest_rank
        FROM latest_match
    ) t
    WHERE latest_rank = 1
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROUND(employee_salary / SUM(employee_salary) OVER (PARTITION BY dept_name), 4) AS employee_salary_share,
        ROW_NUMBER() OVER (ORDER BY budget, id) AS seq_no,
        CUME_DIST() OVER (PARTITION BY level ORDER BY employee_salary DESC) AS employee_salary_cume_dist
    FROM latest
)
-- Multi-level aggregation
SELECT
    seq_no,
    CASE WHEN employee_salary_share >= 0.72 THEN 'HIGH' ELSE 'LOW' END AS employee_salary_share_tier,
    dept_name,
    COUNT(*) AS num_rows,
    COUNT(CASE WHEN dept_name = 'Marketing' THEN 1 END) AS marketing_count,
    COUNT(DISTINCT dept_name) AS distinct_dept_names,
    GROUPING(seq_no) AS grouping_id
FROM metrics
GROUP BY GROUPING SETS ((seq_no, CASE WHEN employee_salary_share >= 0.72 THEN 'HIGH' ELSE 'LOW' END, dept_name), (seq_no, CASE WHEN employee_salary_share >= 0.72 THEN 'HIGH' ELSE 'LOW' END), ())
HAVING num_rows > 1
ORDER BY employee_salary_share_tier
