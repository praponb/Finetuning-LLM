WITH filtered AS (
    SELECT *, CONCAT(location, ' ', dept_name) AS location_dept_name
    FROM departments
    WHERE location IS NULL
),
scored AS (
    SELECT
        *,
        COUNT(budget) OVER (PARTITION BY location ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_budget,
        MIN(budget) OVER (PARTITION BY location ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_budget,
        AVG(budget) OVER (PARTITION BY location ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_budget,
        SUM(budget) OVER (PARTITION BY location ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_budget,
        LAST_VALUE(budget, TRUE) OVER (ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_budget
    FROM filtered
    WHERE location <> 'Berlin'
),
not_flagged AS (
    SELECT s.*
    FROM scored s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees x
            WHERE x.dept_id = s.id
              AND x.email LIKE '%@yahoo.com'
        )
),
latest_match AS (
    SELECT
        s.id,
        s.location_dept_name,
        s.running_count_budget,
        s.running_min_budget,
        s.running_avg_budget,
        s.running_sum_budget,
        s.last_known_budget,
        top1.job_title,
        top1.salary
    FROM not_flagged s
    LEFT JOIN LATERAL (
            SELECT c.job_title, c.salary
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
with_latest AS (
    SELECT
        s.id,
        s.location_dept_name,
        s.running_count_budget,
        s.running_min_budget,
        s.job_title,
        s.salary,
        latest.employee_salary
    FROM latest_match s
    LEFT JOIN LATERAL (
            SELECT c.salary AS employee_salary
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
matched_keys AS (
    SELECT id
    FROM with_latest
    EXCEPT DISTINCT
    SELECT id
    FROM departments
    WHERE budget > 1617000
),
verified AS (
    SELECT s.*
    FROM with_latest s
    INNER JOIN matched_keys k ON s.id = k.id
),
stats AS (
    SELECT
        CASE WHEN salary >= 147000 THEN 'PREMIUM' ELSE 'STANDARD' END AS salary_tier,
        job_title,
        SIZE(COLLECT_SET(job_title)) AS num_job_titles,
        COUNT(DISTINCT job_title) AS distinct_job_titles,
        COUNT_IF(job_title = 'Analyst') AS analyst_events,
        SIZE(COLLECT_SET(job_title)) AS num_job_titles_2,
        COUNT(DISTINCT id) AS distinct_ids
    FROM verified
    GROUP BY 1, 2
)
SELECT
    salary_tier,
    job_title,
    num_job_titles,
    distinct_job_titles,
    analyst_events,
    num_job_titles_2,
    distinct_ids
FROM stats
