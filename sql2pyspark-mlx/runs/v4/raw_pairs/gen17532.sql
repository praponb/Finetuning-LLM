WITH with_latest AS (
    SELECT s.*, x.employee_hire_date
    FROM departments s
    LEFT JOIN LATERAL (
            SELECT c.hire_date AS employee_hire_date
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
metrics AS (
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY location ORDER BY budget NULLS FIRST) budget_dense_rank,
        AVG(budget) OVER w_running budget_avg_2_prior
    FROM with_latest
    WINDOW w_running AS (PARTITION BY location ORDER BY budget, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING)
),
kept AS (
    SELECT s.*
    FROM metrics s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees ch
            WHERE ch.dept_id = s.id
              AND ch.dept_id = 15
        )
),
metrics_departments AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget DESC, id) event_seq,
        AVG(budget_avg_2_prior) OVER (PARTITION BY dept_name ORDER BY budget, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) budget_avg_2_prior_avg_6,
        ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget_avg_2_prior, id) seq_no,
        ROUND(budget_dense_rank / SUM(budget_dense_rank) OVER (PARTITION BY location), 4) budget_dense_rank_share
    FROM kept
)
SELECT
    id,
    dept_name,
    location,
    budget,
    employee_hire_date,
    budget_dense_rank,
    budget_avg_2_prior,
    event_seq,
    budget_avg_2_prior_avg_6,
    seq_no,
    budget_dense_rank_share,
    NTILE(3) OVER (PARTITION BY dept_name ORDER BY budget_dense_rank, id) budget_dense_rank_bucket,
    LAST_VALUE(dept_name, TRUE) OVER w_grp last_known_dept_name,
    AVG(seq_no) OVER w_recent seq_no_avg_3_prior
FROM metrics_departments
WINDOW w_grp AS (PARTITION BY location ORDER BY seq_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_recent AS (ORDER BY budget, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING);
