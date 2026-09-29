WITH filtered AS (
    SELECT
        id,
        full_name,
        email,
        dept_id,
        manager_id,
        job_title,
        hire_date,
        is_remote,
        SUBSTRING(job_title, 1, 3) AS job_title_prefix
    FROM employees
    WHERE job_title IS DISTINCT FROM 'Engineer'
),
enriched AS (
    SELECT
        s.id,
        s.dept_id,
        s.manager_id,
        s.job_title,
        s.hire_date,
        s.is_remote,
        s.job_title_prefix,
        b.dept_name,
        b.budget
    FROM filtered s
    LEFT JOIN departments b
        ON s.dept_id = b.id
),
ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY hire_date NULLS LAST, id) AS dedup_rank
    FROM enriched
    QUALIFY dedup_rank = 1
),
latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY hire_date DESC NULLS LAST, id) AS row_num
    FROM ranked
    QUALIFY row_num = 1
),
enriched_employees AS (
    SELECT
        id,
        manager_id,
        job_title,
        hire_date,
        dept_name,
        row_num,
        ROUND(budget / SUM(budget) OVER (PARTITION BY job_title), 4) AS budget_share,
        ROUND(budget / SUM(budget) OVER (PARTITION BY manager_id), 4) AS budget_share_2,
        ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY budget, id) AS row_seq,
        LAG(budget, 1) OVER (PARTITION BY dept_name, manager_id ORDER BY row_num, id) AS prev_budget
    FROM latest
),
daily AS (
    SELECT
        manager_id,
        CASE WHEN budget_share >= 0.8 THEN 'LARGE' ELSE 'SMALL' END AS budget_share_tier,
        COUNT(DISTINCT dept_name) AS distinct_dept_names,
        ROUND(SUM(row_seq) / NULLIF(COUNT(CASE WHEN dept_name = 'Engineering' THEN 1 END), 0), 2) AS row_seq_per_engineering,
        COUNT_IF(dept_name = 'Legal') AS legal_events,
        MAX(budget_share_2) AS max_budget_share_2,
        APPROX_PERCENTILE(budget_share_2, ARRAY(0.25, 0.5, 0.75)) AS budget_share_2_quartiles,
        COUNT(CASE WHEN job_title = 'Designer' THEN 1 END) AS designer_count
    FROM enriched_employees
    GROUP BY manager_id, CASE WHEN budget_share >= 0.8 THEN 'LARGE' ELSE 'SMALL' END
)
SELECT
    manager_id,
    budget_share_tier,
    distinct_dept_names,
    row_seq_per_engineering,
    legal_events,
    max_budget_share_2,
    budget_share_2_quartiles,
    designer_count
FROM daily
ORDER BY distinct_dept_names DESC
