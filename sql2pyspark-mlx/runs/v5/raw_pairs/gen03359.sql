WITH not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM departments s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees ch
            WHERE ch.dept_id = s.id
              AND ch.dept_id IN (8, 15, 35)
        )
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(budget) OVER (ORDER BY budget, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS budget_avg_6_prior,
        MAX(budget) OVER (PARTITION BY dept_name) AS dept_name_max_budget
    FROM not_flagged
    WHERE location = 'Bangkok'
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        dept_name,
        location,
        budget_avg_6_prior,
        dept_name_max_budget,
        ROW_NUMBER() OVER w_grp AS row_seq,
        NTILE(5) OVER (PARTITION BY dept_name ORDER BY budget, id) AS budget_bucket,
        CUME_DIST() OVER w_running AS budget_avg_6_prior_cume_dist
    FROM metrics
    WINDOW w_grp AS (ORDER BY budget_avg_6_prior, id), w_running AS (ORDER BY budget_avg_6_prior NULLS FIRST)
),
latest_match AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.employee_hire_date
    FROM enriched s
    JOIN LATERAL (
            SELECT c.hire_date AS employee_hire_date
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM latest_match s
    WHERE EXISTS (
            SELECT 1
            FROM employees ch
            WHERE ch.dept_id = s.id
              AND ch.hire_date <= s.employee_hire_date
        )
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        dept_name,
        budget_avg_6_prior,
        dept_name_max_budget,
        row_seq,
        budget_avg_6_prior_cume_dist,
        employee_hire_date,
        CAST(budget_bucket AS DOUBLE) AS budget_bucket_str,
        ADD_MONTHS(employee_hire_date, 1) AS employee_hire_date_plus_months
    FROM eligible
    WHERE location IS NOT NULL
),
metrics_departments AS (
    -- Ranking and gap features
    SELECT
        id,
        dept_name,
        row_seq,
        budget_avg_6_prior_cume_dist,
        employee_hire_date,
        employee_hire_date_plus_months,
        STDDEV_SAMP(dept_name_max_budget) OVER w_grp AS dept_name_stddev_samp_dept_name_max_budget,
        ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY budget_avg_6_prior DESC, id) AS row_seq_2
    FROM cleaned
    WINDOW w_grp AS (PARTITION BY dept_name)
)
SELECT
    id,
    dept_name,
    row_seq,
    budget_avg_6_prior_cume_dist,
    employee_hire_date,
    employee_hire_date_plus_months,
    row_seq_2
FROM metrics_departments
