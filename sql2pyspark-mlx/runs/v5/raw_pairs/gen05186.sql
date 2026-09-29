WITH joined AS (
    -- Attach employees attributes
    SELECT
        a.id,
        a.email,
        a.dept_id,
        a.manager_id,
        a.hire_date,
        a.is_remote,
        e.email AS employee_email,
        e.full_name AS employee_full_name,
        e.job_title AS employee_job_title
    FROM employees a
    LEFT JOIN employees e
        ON a.manager_id = e.id
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, RANK() OVER (PARTITION BY dept_id, employee_job_title ORDER BY hire_date DESC) AS rn
    FROM joined
    QUALIFY rn = 1
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        email,
        dept_id,
        manager_id,
        hire_date,
        is_remote,
        employee_full_name,
        rn,
        LAST_VALUE(employee_job_title, TRUE) OVER (PARTITION BY employee_job_title ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_employee_job_title,
        FIRST_VALUE(employee_job_title, TRUE) OVER (PARTITION BY dept_id ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_employee_job_title,
        ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY rn, id) AS seq_no
    FROM deduped
    WHERE employee_full_name LIKE 'C%'
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY dept_id, manager_id ORDER BY seq_no) AS seq_no_percent_rank,
        PERCENT_RANK() OVER (PARTITION BY dept_id ORDER BY seq_no DESC NULLS FIRST) AS seq_no_percent_rank_2
    FROM metrics
)
SELECT
    id,
    email,
    hire_date,
    is_remote,
    employee_full_name,
    rn,
    first_known_employee_job_title,
    seq_no,
    seq_no_percent_rank
FROM scored
ORDER BY seq_no_percent_rank, hire_date
