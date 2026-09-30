WITH scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(salary) OVER w_recent salary_avg_6,
        ROW_NUMBER() OVER (PARTITION BY job_title, manager_id ORDER BY salary, id) row_seq,
        LAST_VALUE(level) IGNORE NULLS OVER (PARTITION BY job_title ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_level,
        FIRST_VALUE(job_title) IGNORE NULLS OVER (PARTITION BY job_title ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_job_title
    FROM employees
    WINDOW w_recent AS (PARTITION BY manager_id ORDER BY hire_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        full_name,
        email,
        dept_id,
        job_title,
        salary,
        hire_date,
        level,
        last_known_level,
        salary * row_seq salary_times_row_seq
    FROM scored
    WHERE hire_date > TO_DATE('2023-04-04')
      AND job_title IS NOT NULL
      AND salary_avg_6 < 114906
),
scored_employees AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(salary) OVER (ORDER BY hire_date, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) salary_avg_5,
        FIRST_VALUE(level, TRUE) OVER (ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_level,
        AVG(salary) OVER (PARTITION BY dept_id) dept_id_avg_salary
    FROM cleaned
),
cleaned_employees AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        full_name,
        email,
        job_title,
        salary,
        hire_date,
        level,
        salary_times_row_seq,
        salary_avg_5,
        first_known_level,
        dept_id_avg_salary,
        UPPER(level) level_upper
    FROM scored_employees
    WHERE salary_avg_5 <> 147635
),
grouped AS (
    -- Grouped metrics
    SELECT
        DATE_TRUNC('QUARTER', hire_date) AS quarter_start,
        level,
        CASE WHEN salary_times_row_seq >= 76 THEN 'HIGH' ELSE 'LOW' END AS salary_times_row_seq_tier,
        SORT_ARRAY(COLLECT_SET(job_title)) AS job_title_list,
        SUM(CASE WHEN level = 'L2' THEN salary ELSE 0 END) AS l2_salary,
        COUNT(*) AS event_count,
        COUNT(CASE WHEN level = 'L5' THEN 1 END) AS l5_count
    FROM cleaned_employees
    GROUP BY quarter_start, level, salary_times_row_seq_tier
    HAVING l2_salary < 0
)
SELECT
    quarter_start,
    level,
    salary_times_row_seq_tier,
    job_title_list,
    event_count
FROM grouped
ORDER BY level
