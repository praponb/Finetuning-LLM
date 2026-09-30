WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN job_title IN ('Intern', 'Manager') THEN 'Tier 1' ELSE 'Other' END job_title_group,
        COALESCE(job_title, 'UNKNOWN') job_title_clean
    FROM employees
    WHERE level IS NOT NULL
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY level ORDER BY salary, id) AS row_num
    FROM cleaned
    QUALIFY row_num = 1
),
joined AS (
    -- Attach employees attributes
    SELECT
        e.*,
        p2.hire_date AS employee_hire_date,
        p2.full_name AS employee_full_name
    FROM latest e
    LEFT JOIN employees p2
        ON e.manager_id = p2.id
    WHERE LENGTH(p2.email) < 10
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        full_name,
        email,
        job_title,
        salary,
        job_title_group,
        job_title_clean,
        row_num,
        employee_hire_date,
        employee_full_name,
        CONCAT(job_title, '/', full_name) job_title_full_name,
        TRIM(email) clean_email
    FROM joined
    WHERE hire_date >= DATE '2023-02-09'
      AND row_num = 1
)
-- Aggregate with conditional metrics
SELECT
    job_title,
    CASE WHEN salary >= 141000 THEN 'PREMIUM' ELSE 'STANDARD' END AS salary_tier,
    SUM(CASE WHEN job_title = 'Manager' THEN salary ELSE 0 END) AS manager_salary,
    SORT_ARRAY(COLLECT_SET(job_title)) AS job_title_list,
    ROUND(AVG(salary), 2) AS avg_salary,
    STDDEV_SAMP(salary) AS salary_stddev,
    STDDEV_SAMP(salary) AS salary_stddev_2,
    ROUND(AVG(salary), 2) AS avg_salary_2,
    GROUPING(job_title) AS grouping_level
FROM base
GROUP BY GROUPING SETS ((job_title, CASE WHEN salary >= 141000 THEN 'PREMIUM' ELSE 'STANDARD' END), (job_title), ());
