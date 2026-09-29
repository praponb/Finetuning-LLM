WITH cleaned AS (
    SELECT
        id,
        email,
        dept_id,
        manager_id,
        job_title,
        salary,
        is_remote,
        level,
        LENGTH(full_name) AS full_name_length
    FROM employees
    WHERE hire_date > DATE '2023-06-14'
)
SELECT *
FROM (
    SELECT
        dept_id,
        level,
        salary
    FROM cleaned
) src
PIVOT (
    SUM(salary) AS amount
    FOR level IN ('L5', 'L2')
);
