WITH enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER (PARTITION BY dept_name ORDER BY budget ASC NULLS LAST) AS budget_rank,
        STDDEV_SAMP(budget) OVER w_hist AS location_stddev_samp_budget,
        LAG(budget, 2) OVER w_hist_2 AS prev_budget,
        MAX(budget) OVER w_hist AS location_max_budget
    FROM departments
    WHERE dept_name = 'Engineering'
    WINDOW w_hist AS (PARTITION BY location), w_hist_2 AS (PARTITION BY location ORDER BY budget ASC, id ASC)
),
with_last AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.location,
        s.budget,
        s.location_stddev_samp_budget,
        s.prev_budget,
        x.employee_level,
        x.employee_salary
    FROM enriched s
    LEFT JOIN LATERAL (
            SELECT c.level AS employee_level, c.salary AS employee_salary
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
with_latest AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.budget,
        s.prev_budget,
        s.employee_salary,
        top1.employee_job_title,
        top1.level
    FROM with_last s
    LEFT JOIN LATERAL (
            SELECT c.job_title AS employee_job_title, c.level
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
with_last_departments AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.prev_budget,
        s.employee_job_title,
        s.level,
        latest.employee_level,
        latest.employee_salary_2
    FROM with_latest s
    LEFT JOIN LATERAL (
            SELECT c.level AS employee_level, c.salary AS employee_salary_2
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_last_departments s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees ch
            WHERE ch.dept_id = s.id
              AND ch.full_name LIKE 'M%'
        )
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(employee_salary_2) OVER w_part AS next_employee_salary_2,
        LAG(employee_salary_2) OVER w_part AS prev_employee_salary_2,
        LEAD(prev_budget) OVER w_part AS next_prev_budget,
        ROW_NUMBER() OVER w_part AS event_seq,
        SUM(prev_budget) OVER (PARTITION BY employee_job_title ORDER BY employee_salary_2 ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_prev_budget
    FROM not_flagged
    WINDOW w_part AS (PARTITION BY employee_level ORDER BY employee_salary_2 ASC, id ASC)
)
SELECT
    id,
    prev_budget,
    employee_job_title,
    employee_level,
    employee_salary_2,
    prev_employee_salary_2,
    event_seq,
    CASE WHEN employee_level RLIKE '^(L4|L3)$' THEN 'PRIORITY' WHEN employee_level RLIKE '^(L1|L2)$' THEN 'EXTENDED' ELSE 'OTHER_ZONE' END AS employee_level_zone
FROM metrics;
