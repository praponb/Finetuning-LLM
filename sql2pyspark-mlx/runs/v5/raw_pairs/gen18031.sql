WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        DATE_SUB(hire_date, 67) AS hire_date_minus_67d,
        SPLIT(LOWER(email), '@')[0] AS email_user
    FROM employees
    WHERE hire_date >= DATE '2023-02-13'
      AND job_title IS DISTINCT FROM 'Intern'
),
matched_keys AS (
    -- Set difference on keys
    SELECT manager_id, id
    FROM filtered
    EXCEPT
    SELECT manager_id, id
    FROM employees
    WHERE hire_date BETWEEN '2024-08-01' AND '2025-01-01'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered s
    JOIN matched_keys k ON s.manager_id = k.manager_id AND s.id = k.id
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER (ORDER BY salary DESC) AS salary_rank,
        DENSE_RANK() OVER (ORDER BY salary DESC) AS salary_dense_rank,
        STDDEV_SAMP(salary) OVER (PARTITION BY job_title) AS job_title_stddev_samp_salary,
        DENSE_RANK() OVER (PARTITION BY manager_id ORDER BY salary DESC) AS salary_dense_rank_2,
        STDDEV_SAMP(salary) OVER (PARTITION BY job_title, manager_id) AS job_title_stddev_samp_salary_2
    FROM kept_rows
    WHERE UPPER(TRIM(email)) = 'JACK@STARTUP.IO'
),
scored AS (
    -- Ranking and gap features
    SELECT
        full_name,
        email,
        dept_id,
        manager_id,
        hire_date,
        email_user,
        salary_rank,
        salary_dense_rank,
        ROUND(job_title_stddev_samp_salary / SUM(job_title_stddev_samp_salary) OVER (PARTITION BY dept_id), 4) AS job_title_stddev_samp_salary_share,
        CUME_DIST() OVER (PARTITION BY job_title ORDER BY job_title_stddev_samp_salary DESC NULLS LAST) AS job_title_stddev_samp_salary_cume_dist,
        DENSE_RANK() OVER (ORDER BY job_title_stddev_samp_salary_2 NULLS FIRST) AS job_title_stddev_samp_salary_2_dense_rank,
        PERCENT_RANK() OVER (PARTITION BY dept_id ORDER BY salary_dense_rank DESC NULLS FIRST) AS salary_dense_rank_percent_rank,
        RANK() OVER (ORDER BY salary_dense_rank_2 DESC) AS salary_dense_rank_2_rank
    FROM enriched
),
enriched_employees AS (
    -- Attach departments attributes
    SELECT
        src.dept_id,
        src.manager_id,
        src.email_user,
        src.salary_rank,
        src.salary_dense_rank,
        src.job_title_stddev_samp_salary_2_dense_rank,
        src.salary_dense_rank_2_rank,
        p.dept_name,
        p.location,
        p.budget
    FROM scored src
    LEFT JOIN departments p
        ON src.dept_id = p.id
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CAST(salary_dense_rank_2_rank AS DOUBLE) AS salary_dense_rank_2_rank_num,
        COALESCE(location, 'N/A') AS location_clean
    FROM enriched_employees
    WHERE location IS DISTINCT FROM 'New York'
      AND salary_dense_rank_2_rank != 4
),
daily AS (
    -- Grouped metrics
    SELECT
        location,
        dept_name,
        dept_id,
        COUNT(DISTINCT location) AS distinct_locations,
        COUNT(*) AS row_count,
        COUNT(*) AS cnt
    FROM valid
    GROUP BY location, dept_name, dept_id
    HAVING distinct_locations >= 1
)
SELECT
    location,
    dept_name,
    dept_id,
    distinct_locations,
    row_count,
    cnt
FROM daily
ORDER BY dept_name DESC NULLS LAST
