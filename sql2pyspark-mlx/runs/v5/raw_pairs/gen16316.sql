WITH base AS (
    SELECT
        id,
        email,
        dept_id,
        job_title,
        salary,
        hire_date,
        is_remote,
        level,
        CONCAT_WS(',', level, job_title) AS level_job_title_key,
        CEIL(salary) AS salary_whole
    FROM employees
    WHERE job_title IS NOT NULL
      AND manager_id = 35
),
valid AS (
    SELECT *, CASE WHEN level = 'L5' THEN 1 ELSE 0 END AS is_l5
    FROM base
    WHERE YEAR(hire_date) = 2024
),
with_windows AS (
    SELECT
        *,
        LAG(salary_whole, 1, 0) OVER (PARTITION BY job_title ORDER BY salary_whole ASC, id ASC) AS prev_salary_whole,
        AVG(salary) OVER (PARTITION BY job_title, level ORDER BY hire_date ASC, id ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS salary_avg_2,
        SUM(is_l5) OVER (ORDER BY salary_whole ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_is_l5,
        ROW_NUMBER() OVER (ORDER BY salary DESC, id ASC) AS seq_no
    FROM valid
),
scored AS (
    SELECT
        id,
        email,
        dept_id,
        salary,
        is_remote,
        level_job_title_key,
        salary_whole,
        is_l5,
        prev_salary_whole,
        salary_avg_2,
        cumulative_is_l5,
        AVG(salary_whole) OVER w_running AS salary_whole_avg_5,
        LAST_VALUE(salary_avg_2) IGNORE NULLS OVER w_recent AS last_known_salary_avg_2
    FROM with_windows
    WINDOW w_running AS (ORDER BY salary_avg_2 ASC, id ASC ROWS BETWEEN 5 PRECEDING AND CURRENT ROW), w_recent AS (PARTITION BY level ORDER BY seq_no ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
base_employees AS (
    SELECT
        *,
        FLOOR(last_known_salary_avg_2) AS last_known_salary_avg_2_whole,
        ROUND(last_known_salary_avg_2 / salary_avg_2, 2) AS last_known_salary_avg_2_per_salary_avg_2
    FROM scored
    WHERE UPPER(TRIM(email)) = 'IVY@GMAIL.COM'
),
enriched AS (
    SELECT
        id,
        salary,
        level_job_title_key,
        salary_whole,
        is_l5,
        salary_avg_2,
        salary_whole_avg_5,
        last_known_salary_avg_2_whole,
        last_known_salary_avg_2_per_salary_avg_2,
        ROW_NUMBER() OVER (ORDER BY last_known_salary_avg_2 ASC, id ASC) AS event_seq,
        SUM(salary_whole_avg_5) OVER (PARTITION BY dept_id ORDER BY cumulative_is_l5 ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_salary_whole_avg_5
    FROM base_employees
),
enriched_employees AS (
    SELECT
        *,
        NTILE(5) OVER (ORDER BY last_known_salary_avg_2_whole ASC, id ASC) AS last_known_salary_avg_2_whole_bucket,
        LAST_VALUE(event_seq) IGNORE NULLS OVER (ORDER BY cumulative_salary_whole_avg_5 ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_seq
    FROM enriched
)
SELECT
    *,
    ROW_NUMBER() OVER (ORDER BY is_l5 ASC, id ASC) AS row_seq,
    NTILE(10) OVER (ORDER BY salary_avg_2 DESC, id ASC) AS salary_avg_2_bucket,
    RANK() OVER (ORDER BY is_l5 ASC NULLS LAST) AS is_l5_rank
FROM enriched_employees
WHERE last_known_salary_avg_2_whole_bucket BETWEEN 3 AND 5
