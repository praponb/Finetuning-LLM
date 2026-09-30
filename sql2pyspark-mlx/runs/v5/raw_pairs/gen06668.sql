WITH metrics AS (
    SELECT
        id,
        dept_name,
        location,
        LAG(budget) OVER w_hist AS prev_budget,
        LEAD(budget) OVER w_hist AS next_budget,
        ROW_NUMBER() OVER w_hist AS row_seq
    FROM departments
    WINDOW w_hist AS (PARTITION BY dept_name ORDER BY budget, id)
),
eligible AS (
    SELECT s.*
    FROM metrics s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees ch
            WHERE ch.dept_id = s.id
              AND ch.email LIKE '%@gmail.com'
        )
),
with_latest AS (
    SELECT
        s.*,
        latest.salary,
        latest.level
    FROM eligible s
    JOIN LATERAL (
            SELECT c.salary, c.level
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
scored AS (
    SELECT
        id,
        location,
        prev_budget,
        next_budget,
        row_seq,
        FIRST_VALUE(salary, TRUE) OVER (PARTITION BY location ORDER BY prev_budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_salary,
        ROW_NUMBER() OVER w_part AS seq_no,
        NTILE(5) OVER (PARTITION BY location ORDER BY next_budget, id) AS next_budget_bucket
    FROM with_latest
    WINDOW w_part AS (ORDER BY prev_budget DESC, id)
),
metrics_departments AS (
    SELECT
        *,
        LAG(prev_budget) OVER w_hist AS prev_prev_budget,
        LEAD(prev_budget) OVER w_hist AS next_prev_budget,
        LEAD(first_known_salary) OVER w_hist AS next_first_known_salary,
        ROW_NUMBER() OVER w_hist AS event_seq,
        SUM(prev_budget) OVER w_recent AS location_sum_prev_budget
    FROM scored
    WHERE first_known_salary < 112000
    WINDOW w_hist AS (PARTITION BY location ORDER BY next_budget_bucket, id), w_recent AS (PARTITION BY location)
)
SELECT
    id,
    prev_budget,
    next_budget,
    first_known_salary,
    seq_no,
    prev_prev_budget,
    next_prev_budget,
    event_seq,
    location_sum_prev_budget
FROM metrics_departments
ORDER BY id, event_seq;
