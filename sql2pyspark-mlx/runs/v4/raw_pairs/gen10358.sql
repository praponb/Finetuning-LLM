WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        ROUND(salary, 2) salary_rounded,
        LENGTH(job_title) job_title_length
    FROM employees
    WHERE hire_date > TO_DATE('2024-03-08')
      AND job_title IS NOT NULL
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        full_name,
        email,
        dept_id,
        manager_id,
        job_title,
        salary,
        hire_date,
        is_remote,
        level,
        salary_rounded,
        job_title_length,
        ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY job_title_length DESC, id) event_seq,
        LEAD(job_title_length, 1) OVER (PARTITION BY manager_id ORDER BY salary, id) next_job_title_length
    FROM filtered
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        full_name,
        dept_id,
        manager_id,
        job_title,
        salary,
        hire_date,
        level,
        job_title_length,
        event_seq,
        CAST(salary AS INT) salary_int,
        COALESCE(job_title, 'UNKNOWN') job_title_clean
    FROM scored
    WHERE hire_date >= TO_DATE('2024-02-15')
      AND job_title IS NOT NULL
),
scored_employees AS (
    -- Ranking and gap features
    SELECT
        *,
        MIN(job_title_length) OVER (PARTITION BY level, dept_id) level_min_job_title_length,
        ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY salary DESC, id) row_seq
    FROM valid
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        manager_id,
        level,
        level_min_job_title_length
    FROM scored_employees
) src
PIVOT (
    AVG(level_min_job_title_length) AS value_sum,
    COUNT(1) AS n
    FOR level IN ('L3' AS l3, 'L1' AS l1)
)
