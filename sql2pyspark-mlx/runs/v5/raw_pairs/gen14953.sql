WITH filtered AS (
    SELECT
        *,
        CASE WHEN budget >= 3359000 THEN 'High' WHEN budget >= 900000 THEN 'Medium' ELSE 'Low' END AS budget_level,
        CASE WHEN location = 'Berlin' THEN 1 ELSE 0 END AS is_berlin
    FROM departments
    WHERE dept_name IS NOT NULL
      AND dept_name != 'Engineering'
),
metrics AS (
    SELECT
        id,
        location,
        budget,
        budget_level,
        is_berlin,
        LEAD(budget, 1) OVER (PARTITION BY location, dept_name ORDER BY budget, id) AS next_budget,
        FIRST_VALUE(dept_name) IGNORE NULLS OVER (PARTITION BY dept_name ORDER BY is_berlin, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_dept_name,
        LEAD(budget, 1) OVER (ORDER BY budget, id) AS next_budget_2,
        MAX(budget) OVER (PARTITION BY location, dept_name) AS location_max_budget
    FROM filtered
),
not_flagged AS (
    SELECT s.*
    FROM metrics s
    WHERE EXISTS (
            SELECT 1
            FROM employees x
            WHERE x.dept_id = s.id
              AND x.manager_id = 32
        )
),
eligible AS (
    SELECT s.*
    FROM not_flagged s
    WHERE EXISTS (
            SELECT 1
            FROM employees x
            WHERE x.dept_id = s.id
              AND x.level IS NULL
        )
),
with_windows AS (
    SELECT
        id,
        location,
        budget_level,
        is_berlin,
        next_budget,
        next_budget_2,
        location_max_budget,
        COUNT(location_max_budget) OVER w_recent AS last4_count_location_max_budget,
        MAX(location_max_budget) OVER w_recent AS last4_max_location_max_budget,
        SUM(location_max_budget) OVER w_recent AS last4_sum_location_max_budget,
        SUM(next_budget) OVER w_recent_2 AS running_next_budget
    FROM eligible
    WHERE budget IS NOT NULL
    WINDOW w_recent AS (PARTITION BY location ORDER BY is_berlin, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), w_recent_2 AS (ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_latest AS (
    SELECT
        s.id,
        s.location,
        s.budget_level,
        s.next_budget,
        s.location_max_budget,
        s.last4_sum_location_max_budget,
        top1.job_title
    FROM with_windows s
    LEFT JOIN LATERAL (
            SELECT c.job_title
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
latest_match AS (
    SELECT
        s.*,
        latest.hire_date,
        latest.employee_job_title
    FROM with_latest s
    LEFT JOIN LATERAL (
            SELECT c.hire_date, c.job_title AS employee_job_title
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
SELECT
    CASE WHEN location_max_budget >= 3919642 THEN 'PREMIUM' ELSE 'STANDARD' END AS location_max_budget_tier,
    DATE_TRUNC('QUARTER', hire_date) AS quarter_start,
    COUNT(*) AS cnt,
    ROUND(SUM(location_max_budget) / NULLIF(COUNT(CASE WHEN location = 'Chiang Mai' THEN 1 END), 0), 2) AS location_max_budget_per_chiang_mai
FROM latest_match
GROUP BY CASE WHEN location_max_budget >= 3919642 THEN 'PREMIUM' ELSE 'STANDARD' END, DATE_TRUNC('QUARTER', hire_date);
