WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        full_name,
        email,
        dept_id,
        manager_id,
        job_title,
        salary,
        hire_date,
        level,
        ROUND(salary, 0) AS salary_rounded,
        CAST(is_remote AS INT) AS is_remote_flag
    FROM employees
    WHERE hire_date >= TO_DATE('2023-10-13')
      AND level IS NOT NULL
)
-- Running and moving aggregates
SELECT
    *,
    AVG(salary_rounded) OVER (PARTITION BY manager_id) AS manager_id_avg_salary_rounded,
    FIRST_VALUE(is_remote_flag) IGNORE NULLS OVER (PARTITION BY manager_id, level ORDER BY salary_rounded, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_is_remote_flag,
    DENSE_RANK() OVER w_grp AS salary_rounded_dense_rank
FROM valid
WINDOW w_grp AS (PARTITION BY level, job_title ORDER BY salary_rounded DESC NULLS LAST);
