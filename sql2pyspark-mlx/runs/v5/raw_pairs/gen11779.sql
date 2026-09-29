WITH filtered AS (
    -- Keep recent, valid rows
    SELECT *, LENGTH(email) AS email_length
    FROM employees
    WHERE manager_id = 11
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(email_length) OVER w_recent AS prev_email_length,
        LEAD(email_length) OVER w_recent AS next_email_length,
        LAG(salary) OVER w_recent AS prev_salary,
        LEAD(salary) OVER w_recent AS next_salary,
        ROW_NUMBER() OVER w_recent AS seq_no
    FROM filtered
    WINDOW w_recent AS (PARTITION BY job_title ORDER BY email_length, id)
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        dept_id,
        job_title,
        email_length,
        prev_email_length,
        prev_salary,
        LAG(prev_salary, 2) OVER (ORDER BY salary, id) AS prev_prev_salary,
        FIRST_VALUE(seq_no) IGNORE NULLS OVER (PARTITION BY job_title ORDER BY prev_email_length, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_seq_no,
        LAST_VALUE(prev_email_length) IGNORE NULLS OVER (PARTITION BY dept_id ORDER BY next_email_length, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_prev_email_length,
        SUM(prev_email_length) OVER (PARTITION BY manager_id) AS manager_id_sum_prev_email_length,
        ROW_NUMBER() OVER (ORDER BY next_salary DESC, id) AS seq_no_2
    FROM metrics
    WHERE prev_salary > 69000
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM scored
    QUALIFY RANK() OVER (PARTITION BY dept_id, job_title ORDER BY prev_prev_salary) = 1
),
enriched AS (
    -- Attach departments attributes
    SELECT
        a.*,
        b.dept_name,
        b.budget,
        b.location
    FROM deduped a
    LEFT JOIN departments b
        ON a.dept_id = b.id
),
metrics_employees AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        dept_id,
        email_length,
        last_known_prev_email_length,
        manager_id_sum_prev_email_length,
        seq_no_2,
        location,
        SUM(seq_no_2) OVER (ORDER BY seq_no_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_seq_no_2,
        RANK() OVER (PARTITION BY job_title ORDER BY first_known_seq_no DESC) AS first_known_seq_no_rank
    FROM enriched
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY location ORDER BY running_seq_no_2, id) AS rank_in_group
    FROM metrics_employees
    QUALIFY rank_in_group = 1
)
SELECT
    id,
    dept_id,
    email_length,
    manager_id_sum_prev_email_length,
    location,
    running_seq_no_2,
    first_known_seq_no_rank,
    rank_in_group
FROM latest
ORDER BY location DESC;
