WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(salary * 1.07, 2) AS adjusted_salary,
        ADD_MONTHS(hire_date, 3) AS hire_date_plus_months
    FROM employees
    WHERE manager_id IS NOT NULL
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY job_title ORDER BY hire_date_plus_months NULLS LAST, id) AS rn
    FROM base
    QUALIFY rn = 1
),
metrics AS (
    -- Ranking and gap features
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
        adjusted_salary,
        hire_date_plus_months,
        rn,
        ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY hire_date DESC, id) AS row_seq,
        AVG(salary) OVER (PARTITION BY level ORDER BY rn, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS salary_avg_6_prior,
        SUM(adjusted_salary) OVER (ORDER BY rn, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_adjusted_salary,
        LAG(adjusted_salary, 1) OVER (PARTITION BY manager_id, dept_id ORDER BY hire_date, id) AS prev_adjusted_salary
    FROM latest
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY job_title ORDER BY prev_adjusted_salary NULLS LAST, id) AS rn_2
    FROM metrics
    QUALIFY rn_2 = 1
),
latest_employees AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM deduped
    QUALIFY RANK() OVER (PARTITION BY manager_id, dept_id ORDER BY rn DESC) = 1
),
base_employees AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(salary_avg_6_prior * 0.9, 2) AS adjusted_salary_avg_6_prior,
        row_seq * salary_avg_6_prior AS row_seq_times_salary_avg_6_prior
    FROM latest_employees
    WHERE hire_date >= DATE '2023-08-21'
),
latest_employees_2 AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM base_employees
    QUALIFY ROW_NUMBER() OVER (PARTITION BY level ORDER BY salary DESC, id) = 1
)
-- Aggregate with conditional metrics
SELECT
    dept_id,
    CASE WHEN adjusted_salary_avg_6_prior >= 65 THEN 'PREMIUM' ELSE 'STANDARD' END AS adjusted_salary_avg_6_prior_tier,
    COUNT(*) AS cnt,
    COUNT(*) FILTER (WHERE level = 'L4') AS l4_rows,
    PERCENTILE_APPROX(row_seq_times_salary_avg_6_prior, 0.9) AS row_seq_times_salary_avg_6_prior_p90,
    RANK() OVER (PARTITION BY dept_id ORDER BY PERCENTILE_APPROX(row_seq_times_salary_avg_6_prior, 0.9) DESC) AS row_seq_times_salary_avg_6_prior_p90_rank
FROM latest_employees_2
GROUP BY dept_id, CASE WHEN adjusted_salary_avg_6_prior >= 65 THEN 'PREMIUM' ELSE 'STANDARD' END
HAVING PERCENTILE_APPROX(row_seq_times_salary_avg_6_prior, 0.9) > 0;
