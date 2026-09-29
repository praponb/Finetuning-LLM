WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        full_name,
        email,
        dept_id,
        manager_id,
        job_title,
        salary,
        level,
        SUBSTRING(level, 1, 3) AS level_prefix
    FROM employees
    WHERE hire_date > DATE '2023-06-15'
      AND job_title IS NOT NULL
),
joined AS (
    -- Attach departments attributes
    SELECT
        src.*,
        d.budget,
        d.dept_name
    FROM cleaned src
    LEFT JOIN departments d
        ON src.dept_id = d.id
    WHERE d.dept_name = 'HR'
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        full_name,
        dept_id,
        job_title,
        salary,
        budget,
        dept_name,
        TRIM(dept_name) AS clean_dept_name,
        UPPER(email) AS email_upper,
        COALESCE(job_title, 'DIRECT') AS resolved_job_title
    FROM joined
    WHERE dept_name NOT IN ('Engineering', 'HR', 'Marketing')
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        job_title,
        salary,
        budget,
        dept_name,
        clean_dept_name,
        email_upper,
        resolved_job_title,
        SUM(salary) OVER (ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_salary,
        AVG(salary) OVER (ORDER BY budget, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS salary_avg_5_prior
    FROM filtered
),
cleaned_employees AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CONCAT(job_title, ', ', dept_name) AS job_title_dept_name,
        COALESCE(dept_name, 'OTHER') AS resolved_dept_name
    FROM scored
    WHERE dept_name = 'Legal'
),
stats AS (
    -- Grouped metrics
    SELECT
        CASE WHEN salary >= 149000 THEN 'PREMIUM' ELSE 'STANDARD' END AS salary_tier,
        COUNT(*) FILTER (WHERE dept_name = 'Sales') AS sales_rows,
        COUNT(*) AS row_count,
        SUM(CASE WHEN dept_name = 'Marketing' THEN salary_avg_5_prior ELSE 0 END) AS marketing_salary_avg_5_prior,
        SUM(CASE WHEN job_title = 'Engineer' THEN salary ELSE 0 END) AS engineer_salary,
        PERCENTILE_APPROX(budget, ARRAY(0.25, 0.5, 0.75)) AS budget_quartiles,
        SUM(CASE WHEN job_title = 'Intern' THEN salary ELSE 0 END) AS intern_salary
    FROM cleaned_employees
    GROUP BY salary_tier
)
SELECT
    salary_tier,
    sales_rows,
    row_count,
    marketing_salary_avg_5_prior,
    budget_quartiles,
    intern_salary
FROM stats
