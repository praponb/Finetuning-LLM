WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CAST(budget AS INT) AS budget_int,
        LOWER(location) AS location_lower,
        COALESCE(dept_name, 'NONE') AS resolved_dept_name
    FROM departments
    WHERE dept_name IS NOT NULL
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        RANK() OVER (PARTITION BY dept_name ORDER BY budget_int DESC) AS budget_int_rank,
        LEAD(budget_int, 1) OVER w_ordered AS next_budget_int,
        NTILE(10) OVER (ORDER BY budget_int, id) AS budget_int_bucket
    FROM valid
    WINDOW w_ordered AS (PARTITION BY dept_name ORDER BY budget, id)
),
with_latest AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.budget,
        s.budget_int,
        s.location_lower,
        s.resolved_dept_name,
        s.budget_int_rank,
        s.budget_int_bucket,
        latest.employee_level,
        latest.employee_salary
    FROM metrics s
    LEFT JOIN LATERAL (
            SELECT c.level AS employee_level, c.salary AS employee_salary
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM with_latest
    QUALIFY ROW_NUMBER() OVER (PARTITION BY employee_level ORDER BY budget_int_rank DESC, id) = 1
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        resolved_dept_name,
        budget_int_bucket,
        employee_level,
        employee_salary,
        LEAD(budget_int_bucket) OVER w_part AS next_budget_int_bucket,
        LAG(budget) OVER w_part AS prev_budget,
        LEAD(budget) OVER w_part AS next_budget,
        ROW_NUMBER() OVER w_part AS row_seq
    FROM latest
    WINDOW w_part AS (PARTITION BY employee_level ORDER BY budget, id)
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN next_budget_int_bucket >= 7 THEN 'HIGH' ELSE 'LOW' END AS next_budget_int_bucket_tier,
    PERCENTILE_APPROX(prev_budget, ARRAY(0.25, 0.5, 0.75)) AS prev_budget_quartiles,
    SIZE(COLLECT_SET(employee_level)) AS num_employee_levels,
    COUNT(CASE WHEN employee_level = 'L2' THEN 1 END) AS l2_count,
    COUNT_IF(employee_level = 'L2') AS l2_events
FROM scored
GROUP BY next_budget_int_bucket_tier
HAVING l2_events >= 1;
