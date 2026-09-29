WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        full_name,
        email,
        dept_id,
        manager_id,
        salary,
        hire_date,
        is_remote,
        ROUND(salary * 0.85, 2) salary_scaled,
        COALESCE(job_title, 'N/A') resolved_job_title
    FROM employees
    WHERE hire_date > DATE '2023-04-03'
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY manager_id ORDER BY salary_scaled DESC, id) AS rn
    FROM filtered
    QUALIFY rn = 1
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        full_name,
        email,
        dept_id,
        manager_id,
        resolved_job_title,
        rn,
        ROW_NUMBER() OVER (PARTITION BY dept_id, manager_id ORDER BY hire_date DESC, id) event_seq,
        SUM(salary_scaled) OVER (PARTITION BY manager_id ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_salary_scaled,
        AVG(salary_scaled) OVER (PARTITION BY manager_id ORDER BY rn, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) salary_scaled_avg_3,
        MIN(salary) OVER (PARTITION BY manager_id) manager_id_min_salary,
        ROUND(salary / SUM(salary) OVER (PARTITION BY dept_id), 4) salary_share
    FROM latest
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM scored
    QUALIFY RANK() OVER (PARTITION BY manager_id, dept_id ORDER BY salary_scaled_avg_3 DESC) = 1
)
SELECT
    s.id,
    s.full_name,
    s.dept_id,
    s.manager_id,
    s.resolved_job_title,
    s.event_seq,
    s.running_salary_scaled,
    s.salary_scaled_avg_3,
    CASE WHEN full_name RLIKE '^(Emma Brown|Ivy Chen)$' THEN 'GROUP_A' WHEN full_name RLIKE '^(Alice Wong|bob smith)$' THEN 'OTHER_ZONE' ELSE 'CORE' END AS full_name_zone,
    (SELECT SUM(e2.salary) FROM employees e2 WHERE e2.manager_id = s.manager_id) AS sum_employee_salary
FROM ranked s
ORDER BY event_seq NULLS FIRST
