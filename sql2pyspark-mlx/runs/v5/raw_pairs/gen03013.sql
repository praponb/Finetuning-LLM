WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        full_name,
        email,
        dept_id,
        job_title,
        salary,
        level,
        IF(is_remote, 'Yes', 'No') AS is_remote_label,
        SUBSTRING(level, 1, 2) AS level_prefix
    FROM employees
    WHERE dept_id = 38
      AND hire_date >= DATE '2023-02-08'
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(salary, 2) OVER (ORDER BY salary, id) AS prev_salary,
        SUM(salary) OVER (ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_salary,
        NTILE(10) OVER (PARTITION BY dept_id, job_title ORDER BY salary, id) AS salary_bucket,
        SUM(salary) OVER w_ordered AS cumulative_salary
    FROM cleaned
    WHERE full_name IS NOT NULL
    WINDOW w_ordered AS (PARTITION BY dept_id ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        full_name,
        email,
        dept_id,
        job_title,
        salary,
        level,
        is_remote_label,
        prev_salary,
        running_salary,
        cumulative_salary
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY job_title, dept_id ORDER BY salary DESC, id) AS latest_rank
        FROM scored
    ) x
    WHERE latest_rank = 1
),
matched_keys AS (
    -- Set difference on keys
    SELECT job_title
    FROM ranked
    EXCEPT DISTINCT
    SELECT job_title
    FROM employees
    WHERE job_title IN ('Intern', 'Analyst', 'Director')
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM ranked s
    INNER JOIN matched_keys k ON s.job_title = k.job_title
)
SELECT
    email,
    job_title,
    salary,
    level,
    is_remote_label,
    prev_salary,
    running_salary,
    CASE WHEN job_title RLIKE '^(Manager|Intern)$' THEN 'GROUP_B' WHEN job_title RLIKE '^(Engineer|Analyst)$' THEN 'CORE' ELSE 'PRIORITY' END AS job_title_zone
FROM verified;
