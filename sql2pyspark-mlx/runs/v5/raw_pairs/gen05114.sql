WITH metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(salary) OVER (PARTITION BY manager_id ORDER BY hire_date ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_avg_salary,
        SUM(salary) OVER (PARTITION BY manager_id ORDER BY hire_date ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_sum_salary,
        MIN(salary) OVER (PARTITION BY manager_id ORDER BY hire_date ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_min_salary,
        ROUND(salary / SUM(salary) OVER (PARTITION BY job_title), 4) AS salary_share
    FROM employees
    WHERE manager_id = 6
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        dept_id,
        manager_id,
        salary,
        hire_date
    FROM metrics
    QUALIFY ROW_NUMBER() OVER (PARTITION BY job_title ORDER BY salary_share DESC, id ASC) = 1
),
metrics_employees AS (
    -- Running and moving aggregates
    SELECT
        id,
        manager_id,
        salary,
        AVG(salary) OVER (PARTITION BY dept_id ORDER BY hire_date ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_avg_salary,
        SUM(salary) OVER (PARTITION BY dept_id ORDER BY hire_date ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_sum_salary,
        COUNT(salary) OVER (PARTITION BY dept_id ORDER BY hire_date ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_count_salary,
        MAX(salary) OVER (PARTITION BY dept_id ORDER BY hire_date ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_max_salary
    FROM deduped
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(last4_count_salary) OVER (PARTITION BY manager_id ORDER BY last4_count_salary ASC, id ASC) AS prev_last4_count_salary,
        LEAD(last4_sum_salary) OVER (PARTITION BY manager_id ORDER BY last4_count_salary ASC, id ASC) AS next_last4_sum_salary,
        LAG(last4_sum_salary) OVER (PARTITION BY manager_id ORDER BY last4_count_salary ASC, id ASC) AS prev_last4_sum_salary,
        ROW_NUMBER() OVER (PARTITION BY manager_id ORDER BY last4_count_salary ASC, id ASC) AS seq_no,
        NTILE(3) OVER (ORDER BY last4_count_salary DESC, id ASC) AS last4_count_salary_bucket
    FROM metrics_employees
),
metrics_employees_2 AS (
    -- Running and moving aggregates
    SELECT
        id,
        salary,
        last4_sum_salary,
        prev_last4_count_salary,
        prev_last4_sum_salary,
        seq_no,
        last4_count_salary_bucket,
        SUM(seq_no) OVER w_ordered AS last4_sum_seq_no,
        MIN(seq_no) OVER w_ordered AS last4_min_seq_no,
        MAX(seq_no) OVER w_ordered AS last4_max_seq_no,
        COUNT(last4_max_salary) OVER w_running AS manager_id_count_last4_max_salary
    FROM with_windows
    WINDOW w_ordered AS (PARTITION BY manager_id ORDER BY last4_avg_salary ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), w_running AS (PARTITION BY manager_id)
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN last4_min_seq_no >= 8.0 THEN 'High' WHEN last4_min_seq_no >= 2.9 THEN 'Medium' ELSE 'Low' END AS last4_min_seq_no_level,
        prev_last4_sum_salary + manager_id_count_last4_max_salary AS prev_last4_sum_salary_plus_manager_id_count_last4_max_salary
    FROM metrics_employees_2
    WHERE last4_sum_seq_no <= 35
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        prev_last4_count_salary,
        seq_no,
        last4_sum_seq_no,
        prev_last4_sum_salary_plus_manager_id_count_last4_max_salary,
        LAG(last4_max_seq_no, 1) OVER (ORDER BY manager_id_count_last4_max_salary ASC, id ASC) AS prev_last4_max_seq_no,
        DENSE_RANK() OVER (ORDER BY last4_count_salary_bucket ASC NULLS FIRST) AS last4_count_salary_bucket_dense_rank,
        ROW_NUMBER() OVER (ORDER BY manager_id_count_last4_max_salary DESC, id ASC) AS seq_no_2,
        NTILE(5) OVER (ORDER BY prev_last4_sum_salary DESC, id ASC) AS prev_last4_sum_salary_bucket,
        SUM(last4_sum_salary) OVER (ORDER BY last4_count_salary_bucket ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_last4_sum_salary
    FROM cleaned
)
SELECT
    id,
    prev_last4_count_salary,
    seq_no,
    last4_sum_seq_no,
    prev_last4_sum_salary_plus_manager_id_count_last4_max_salary,
    prev_last4_max_seq_no,
    seq_no_2,
    running_last4_sum_salary
FROM scored
ORDER BY last4_sum_seq_no DESC
