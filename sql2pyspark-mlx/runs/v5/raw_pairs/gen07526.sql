WITH latest AS (
    SELECT *, RANK() OVER (PARTITION BY manager_id ORDER BY hire_date DESC NULLS LAST) AS rn
    FROM employees
    QUALIFY rn = 1
),
enriched AS (
    SELECT
        *,
        LAG(salary) OVER w_running AS prev_salary,
        LEAD(salary) OVER w_running AS next_salary,
        ROW_NUMBER() OVER w_running AS seq_no,
        STDDEV_SAMP(salary) OVER w_ordered AS manager_id_stddev_samp_salary
    FROM latest
    WINDOW w_running AS (PARTITION BY job_title ORDER BY hire_date, id), w_ordered AS (PARTITION BY manager_id)
),
enriched_employees AS (
    SELECT
        id,
        dept_id,
        manager_id,
        hire_date,
        is_remote,
        seq_no,
        manager_id_stddev_samp_salary,
        LAG(prev_salary) OVER (PARTITION BY dept_id, level ORDER BY manager_id_stddev_samp_salary, id) AS prev_prev_salary,
        LAG(salary) OVER (PARTITION BY dept_id, level ORDER BY manager_id_stddev_samp_salary, id) AS prev_salary_2,
        ROW_NUMBER() OVER (PARTITION BY dept_id, level ORDER BY manager_id_stddev_samp_salary, id) AS row_seq,
        ROUND(seq_no / SUM(seq_no) OVER (PARTITION BY dept_id), 4) AS seq_no_share
    FROM enriched
),
enriched_employees_2 AS (
    SELECT
        a.id,
        a.dept_id,
        a.manager_id,
        a.hire_date,
        a.seq_no,
        a.manager_id_stddev_samp_salary,
        a.prev_salary_2,
        d.budget,
        d.location,
        d.dept_name
    FROM enriched_employees a
    LEFT JOIN departments d
        ON a.dept_id = d.id
),
with_windows AS (
    SELECT
        id,
        dept_id,
        manager_id,
        hire_date,
        seq_no,
        manager_id_stddev_samp_salary,
        budget,
        dept_name,
        MAX(seq_no) OVER w_ordered AS running_max_seq_no,
        SUM(seq_no) OVER w_ordered AS running_sum_seq_no,
        MIN(seq_no) OVER w_ordered AS running_min_seq_no
    FROM enriched_employees_2
    WHERE manager_id_stddev_samp_salary != 105247
    WINDOW w_ordered AS (PARTITION BY dept_name, location ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    *,
    ROW_NUMBER() OVER (ORDER BY hire_date DESC, id) AS seq_no_2,
    LAG(running_sum_seq_no, 2, 0) OVER (PARTITION BY dept_id ORDER BY manager_id_stddev_samp_salary, id) AS prev_running_sum_seq_no,
    ROW_NUMBER() OVER w_recent AS seq_no_3,
    MAX(running_min_seq_no) OVER w_hist AS dept_id_max_running_min_seq_no,
    SUM(running_sum_seq_no) OVER w_hist_2 AS running_running_sum_seq_no
FROM with_windows
WINDOW w_recent AS (PARTITION BY manager_id ORDER BY running_max_seq_no, id), w_hist AS (PARTITION BY dept_id, dept_name), w_hist_2 AS (PARTITION BY manager_id, dept_name ORDER BY running_min_seq_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW);
