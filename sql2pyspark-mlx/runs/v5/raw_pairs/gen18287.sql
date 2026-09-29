WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        INITCAP(TRIM(email)) AS email_title,
        COALESCE(level, 'NONE') AS level_filled
    FROM employees
    WHERE hire_date > DATE '2023-06-07'
      AND job_title IS NOT DISTINCT FROM 'Intern'
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(salary) OVER w_part AS running_sum_salary,
        AVG(salary) OVER w_part AS running_avg_salary,
        COUNT(salary) OVER w_part AS running_count_salary,
        AVG(salary) OVER (PARTITION BY dept_id ORDER BY salary, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS salary_avg_6
    FROM filtered
    WHERE level IN ('L5', 'L4')
    WINDOW w_part AS (PARTITION BY level, manager_id ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_windows_employees AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY salary DESC) AS salary_dense_rank,
        ROW_NUMBER() OVER (ORDER BY hire_date DESC, id) AS event_seq,
        LEAD(salary, 2) OVER (ORDER BY running_count_salary, id) AS next_salary
    FROM with_windows
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        full_name,
        dept_id,
        manager_id,
        job_title,
        hire_date,
        is_remote,
        email_title,
        salary_dense_rank,
        SUM(next_salary) OVER (PARTITION BY job_title, level) AS job_title_sum_next_salary,
        SUM(next_salary) OVER (PARTITION BY manager_id ORDER BY salary_dense_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_next_salary,
        SUM(salary_dense_rank) OVER (PARTITION BY dept_id ORDER BY event_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_salary_dense_rank,
        ROW_NUMBER() OVER (PARTITION BY level ORDER BY running_sum_salary, id) AS event_seq_2
    FROM with_windows_employees
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        manager_id,
        dept_id,
        job_title,
        salary_dense_rank
    FROM scored
) src
PIVOT (
    MAX(salary_dense_rank)
    FOR job_title IN ('Designer', 'Manager')
);
