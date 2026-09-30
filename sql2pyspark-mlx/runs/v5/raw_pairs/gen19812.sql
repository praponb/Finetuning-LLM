WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, SUBSTRING(email, 1, 3) AS email_prefix
    FROM employees
    WHERE level IS DISTINCT FROM 'L5'
      AND hire_date <= '2024-04-01'
),
with_dims AS (
    -- Attach departments attributes
    SELECT
        s.id,
        s.full_name,
        s.dept_id,
        s.manager_id,
        s.job_title,
        s.salary,
        s.is_remote,
        p.location
    FROM cleaned s
    INNER JOIN departments p
        ON s.dept_id = p.id
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM with_dims
    QUALIFY ROW_NUMBER() OVER (PARTITION BY location ORDER BY salary, id) = 1
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        dept_id,
        manager_id,
        is_remote,
        location,
        LAG(salary) OVER (PARTITION BY job_title ORDER BY salary, id) AS prev_salary,
        LEAD(salary) OVER (PARTITION BY job_title ORDER BY salary, id) AS next_salary,
        ROW_NUMBER() OVER (PARTITION BY job_title ORDER BY salary, id) AS event_seq
    FROM latest
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LEAD(event_seq, 1) OVER w_ordered AS next_event_seq,
        RANK() OVER (PARTITION BY location ORDER BY next_salary) AS next_salary_rank,
        SUM(next_salary) OVER w_part AS cumulative_next_salary
    FROM enriched
    WINDOW w_ordered AS (PARTITION BY manager_id ORDER BY prev_salary, id), w_part AS (PARTITION BY manager_id ORDER BY prev_salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_id, location ORDER BY next_event_seq, id) AS row_num
    FROM with_windows
    QUALIFY row_num = 1
)
-- Join a dimension and aggregate
SELECT
    d.location AS department_location,
    e.location AS employee_location,
    COUNT(*) AS rows_total,
    MAX(e.event_seq) AS max_event_seq
FROM ranked e
INNER JOIN departments d ON e.dept_id = d.id
GROUP BY ROLLUP (d.location, e.location)
ORDER BY max_event_seq NULLS FIRST;
