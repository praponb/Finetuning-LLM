WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CONCAT(job_title, '/', level) AS job_title_level,
        ROUND(salary, 2) AS salary_rounded
    FROM employees
    WHERE hire_date > DATE '2023-09-03'
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        STDDEV_SAMP(salary_rounded) OVER (PARTITION BY job_title) AS job_title_stddev_samp_salary_rounded,
        ROW_NUMBER() OVER (PARTITION BY level ORDER BY salary_rounded DESC, id ASC) AS seq_no,
        AVG(salary) OVER (PARTITION BY dept_id) AS dept_id_avg_salary,
        PERCENT_RANK() OVER w_recent AS salary_rounded_percent_rank
    FROM valid
    WINDOW w_recent AS (PARTITION BY job_title, dept_id ORDER BY salary_rounded DESC NULLS FIRST)
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        full_name,
        email,
        manager_id,
        job_title,
        salary,
        hire_date,
        is_remote,
        job_title_level,
        job_title_stddev_samp_salary_rounded,
        seq_no,
        dept_id_avg_salary,
        salary_rounded_percent_rank,
        job_title_stddev_samp_salary_rounded + dept_id_avg_salary AS job_title_stddev_samp_salary_rounded_plus_dept_id_avg_salary
    FROM scored
    WHERE hire_date > DATE '2023-11-01'
      AND job_title IS NOT NULL
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        ROUND(salary / SUM(salary) OVER (PARTITION BY manager_id, job_title), 4) AS salary_share,
        LAG(seq_no, 1) OVER (PARTITION BY job_title ORDER BY seq_no ASC, id ASC) AS prev_seq_no,
        SUM(dept_id_avg_salary) OVER (ORDER BY seq_no ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_dept_id_avg_salary,
        AVG(salary_rounded_percent_rank) OVER (ORDER BY dept_id_avg_salary ASC, id ASC ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS salary_rounded_percent_rank_avg_3_prior,
        ROUND(seq_no / SUM(seq_no) OVER (PARTITION BY manager_id, job_title), 4) AS seq_no_share
    FROM cleaned
)
-- Window metrics over several different partitions
SELECT
    id,
    full_name,
    email,
    manager_id,
    job_title,
    salary,
    hire_date,
    is_remote,
    job_title_level,
    job_title_stddev_samp_salary_rounded,
    seq_no,
    dept_id_avg_salary,
    salary_rounded_percent_rank,
    job_title_stddev_samp_salary_rounded_plus_dept_id_avg_salary,
    salary_share,
    prev_seq_no,
    cumulative_dept_id_avg_salary,
    salary_rounded_percent_rank_avg_3_prior,
    seq_no_share,
    DENSE_RANK() OVER (PARTITION BY job_title ORDER BY salary_share ASC NULLS FIRST) AS salary_share_dense_rank,
    NTILE(3) OVER (ORDER BY cumulative_dept_id_avg_salary ASC, id ASC) AS cumulative_dept_id_avg_salary_bucket,
    ROW_NUMBER() OVER (PARTITION BY job_title ORDER BY salary_rounded_percent_rank DESC, id ASC) AS event_seq
FROM metrics;
