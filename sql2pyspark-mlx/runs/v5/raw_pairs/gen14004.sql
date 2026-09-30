WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        LENGTH(location) AS location_length,
        COALESCE(dept_name, 'DIRECT') AS dept_name_filled
    FROM departments
    WHERE dept_name IS NOT NULL
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        dept_name,
        budget,
        location_length,
        MAX(budget) OVER (PARTITION BY dept_name ORDER BY location_length, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_budget,
        MIN(budget) OVER (PARTITION BY dept_name ORDER BY location_length, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_budget,
        SUM(budget) OVER (PARTITION BY dept_name ORDER BY location_length, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_budget,
        AVG(budget) OVER (PARTITION BY dept_name ORDER BY budget, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS budget_avg_5
    FROM cleaned
    WHERE budget < 3463000
),
metrics_departments AS (
    -- Ranking and gap features
    SELECT
        id,
        dept_name,
        location_length,
        running_max_budget,
        running_sum_budget,
        budget_avg_5,
        SUM(budget_avg_5) OVER (PARTITION BY dept_name ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_budget_avg_5,
        ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY location_length, id) AS event_seq,
        ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY running_max_budget DESC, id) AS seq_no
    FROM metrics
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM metrics_departments s
    WHERE EXISTS (
            SELECT 1
            FROM employees ch
            WHERE ch.dept_id = s.id
              AND ch.dept_id IN (15, 25, 28)
        )
),
with_last AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.dept_name,
        s.running_sum_budget,
        s.running_budget_avg_5,
        s.event_seq,
        s.seq_no,
        latest.job_title,
        latest.employee_salary
    FROM not_flagged s
    JOIN LATERAL (
            SELECT c.job_title, c.salary AS employee_salary
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
SELECT
    id,
    dept_name,
    running_sum_budget,
    running_budget_avg_5,
    event_seq,
    seq_no,
    job_title,
    CASE WHEN dept_name RLIKE '^(Engineering|Sales)$' THEN 'OTHER_ZONE' ELSE 'CORE' END AS dept_name_zone
FROM with_last
ORDER BY seq_no DESC, running_sum_budget
