WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, LENGTH(full_name) full_name_length
    FROM employees
    WHERE level IS NOT NULL
      AND salary >= 147000
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY level, manager_id ORDER BY hire_date DESC) AS rank_in_group
        FROM filtered
    ) t
    WHERE rank_in_group = 1
),
valid AS (
    -- Keep recent, valid rows
    SELECT *, UPPER(TRIM(full_name)) full_name_normalized
    FROM ranked
    WHERE hire_date >= DATE '2023-03-14'
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAST_VALUE(job_title) IGNORE NULLS OVER (PARTITION BY manager_id ORDER BY full_name_length, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_job_title,
        AVG(full_name_length) OVER (PARTITION BY level, dept_id ORDER BY hire_date, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) full_name_length_avg_6_prior,
        NTILE(4) OVER (PARTITION BY dept_id, manager_id ORDER BY full_name_length DESC, id) full_name_length_bucket,
        FIRST_VALUE(full_name_length) IGNORE NULLS OVER (PARTITION BY job_title ORDER BY rank_in_group, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_full_name_length
    FROM valid
),
ranked_employees AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY full_name_length NULLS LAST, id) AS row_num
    FROM scored
    QUALIFY row_num = 1
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        email,
        dept_id,
        manager_id,
        job_title,
        full_name_length,
        last_known_job_title,
        full_name_length_bucket,
        row_num,
        COUNT(full_name_length_avg_6_prior) OVER w_recent running_count_full_name_length_avg_6_prior,
        SUM(full_name_length_avg_6_prior) OVER w_recent running_sum_full_name_length_avg_6_prior,
        MIN(full_name_length_avg_6_prior) OVER w_recent running_min_full_name_length_avg_6_prior
    FROM ranked_employees
    WINDOW w_recent AS (PARTITION BY job_title ORDER BY first_known_full_name_length, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
summary AS (
    -- Multi-level aggregation
    SELECT
        job_title,
        manager_id,
        APPROX_PERCENTILE(full_name_length_bucket, ARRAY(0.25, 0.5, 0.75)) AS full_name_length_bucket_quartiles,
        SUM(CASE WHEN job_title = 'Intern' THEN running_count_full_name_length_avg_6_prior ELSE 0 END) AS intern_running_count_full_name_length_avg_6_prior,
        APPROX_PERCENTILE(running_sum_full_name_length_avg_6_prior, 0.5) AS running_sum_full_name_length_avg_6_prior_p50,
        COUNT(CASE WHEN job_title = 'Director' THEN 1 END) AS director_count,
        COUNT(CASE WHEN job_title = 'Engineer' THEN 1 END) AS engineer_count
    FROM with_windows
    GROUP BY GROUPING SETS ((job_title, manager_id), (job_title), ())
)
SELECT
    s.job_title,
    s.manager_id,
    s.full_name_length_bucket_quartiles,
    s.intern_running_count_full_name_length_avg_6_prior,
    s.running_sum_full_name_length_avg_6_prior_p50,
    s.director_count,
    s.engineer_count,
    (SELECT MAX(x.salary) FROM employees x WHERE x.manager_id = s.manager_id) AS max_employee_salary
FROM summary s;
