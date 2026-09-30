WITH cleaned AS (
    SELECT *, LOWER(email) AS email_lower
    FROM employees
    WHERE manager_id IS NOT NULL
),
latest AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY job_title ORDER BY hire_date DESC, id) AS rank_in_group
        FROM cleaned
    ) t
    WHERE rank_in_group = 1
),
scored AS (
    SELECT
        *,
        LAST_VALUE(job_title) IGNORE NULLS OVER (ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_job_title,
        SUM(salary) OVER w_ordered AS cumulative_salary,
        ROUND(salary / SUM(salary) OVER (PARTITION BY level), 4) AS salary_share
    FROM latest
    WINDOW w_ordered AS (PARTITION BY job_title ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
scored_employees AS (
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY hire_date, id) AS seq_no,
        ROW_NUMBER() OVER w_hist AS seq_no_2
    FROM scored
    WINDOW w_hist AS (ORDER BY hire_date DESC, id)
),
filtered AS (
    SELECT
        *,
        UPPER(full_name) AS full_name_upper,
        COALESCE(job_title, 'OTHER') AS resolved_job_title
    FROM scored_employees
    WHERE hire_date >= DATE '2024-02-20'
      AND cumulative_salary <= 522456
)
SELECT
    level,
    CASE WHEN salary >= 158000 THEN 'HIGH' ELSE 'LOW' END AS salary_tier,
    APPROX_PERCENTILE(seq_no_2, 0.95) AS seq_no_2_p95,
    APPROX_PERCENTILE(seq_no, 0.9) AS seq_no_p90,
    GROUPING_ID() AS grouping_id
FROM filtered
GROUP BY CUBE (level, CASE WHEN salary >= 158000 THEN 'HIGH' ELSE 'LOW' END)
ORDER BY salary_tier
