WITH enriched AS (
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
        MAX(salary) OVER (PARTITION BY level, manager_id) AS level_max_salary,
        ROW_NUMBER() OVER (PARTITION BY job_title ORDER BY hire_date DESC, id) AS event_seq,
        AVG(salary) OVER (PARTITION BY manager_id ORDER BY salary, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS salary_avg_6,
        ROUND(salary / SUM(salary) OVER (PARTITION BY job_title), 4) AS salary_share
    FROM employees
),
enriched_employees AS (
    -- Attach departments attributes
    SELECT
        s.*,
        p.dept_name,
        p.location,
        p.budget
    FROM enriched s
    LEFT JOIN departments p
        ON s.dept_id = p.id
),
joined AS (
    -- Attach employees attributes
    SELECT src.*, d.salary AS employee_salary
    FROM enriched_employees src
    INNER JOIN employees d
        ON src.manager_id = d.id
),
metrics AS (
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
        level_max_salary,
        event_seq,
        salary_avg_6,
        salary_share,
        dept_name,
        location,
        budget,
        employee_salary,
        NTILE(10) OVER (ORDER BY budget, id) AS budget_bucket,
        SUM(budget) OVER (ORDER BY salary_share, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_budget,
        FIRST_VALUE(salary_share, TRUE) OVER (PARTITION BY job_title ORDER BY event_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_salary_share
    FROM joined
),
enriched_employees_2 AS (
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
        level_max_salary,
        event_seq,
        salary_avg_6,
        salary_share,
        dept_name,
        location,
        budget,
        employee_salary,
        budget_bucket,
        cumulative_budget,
        first_known_salary_share,
        AVG(salary_share) OVER (PARTITION BY location ORDER BY salary, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS salary_share_avg_6_prior,
        SUM(employee_salary) OVER (ORDER BY first_known_salary_share, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_employee_salary,
        AVG(salary_avg_6) OVER (ORDER BY salary, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS salary_avg_6_avg_5,
        FIRST_VALUE(salary_share) IGNORE NULLS OVER (PARTITION BY manager_id ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_salary_share_2
    FROM metrics
),
enriched_employees_3 AS (
    -- Ranking and gap features
    SELECT
        *,
        FIRST_VALUE(salary_share, TRUE) OVER (PARTITION BY location, dept_id ORDER BY salary_share_avg_6_prior, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_salary_share_3,
        SUM(salary_avg_6) OVER (PARTITION BY job_title, manager_id ORDER BY salary_share, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_salary_avg_6,
        AVG(salary_avg_6) OVER (PARTITION BY dept_name, dept_id ORDER BY cumulative_budget, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS salary_avg_6_avg_5_prior
    FROM enriched_employees_2
)
SELECT
    id,
    email,
    dept_id,
    job_title,
    is_remote,
    level,
    level_max_salary,
    event_seq,
    budget
FROM enriched_employees_3
ORDER BY level NULLS LAST, id DESC
LIMIT 5;
