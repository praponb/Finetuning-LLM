WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        full_name,
        manager_id,
        salary,
        level,
        CASE WHEN job_title = 'Manager' THEN 1 ELSE 0 END is_manager,
        LENGTH(job_title) job_title_length
    FROM employees
    WHERE dept_id IN (4, 31)
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(job_title_length) OVER (PARTITION BY manager_id, level ORDER BY job_title_length ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_job_title_length,
        NTILE(4) OVER (ORDER BY salary DESC, id ASC) salary_bucket,
        CUME_DIST() OVER (PARTITION BY manager_id ORDER BY job_title_length DESC NULLS FIRST) job_title_length_cume_dist
    FROM filtered
),
joined AS (
    -- Attach employees attributes
    SELECT
        a.*,
        p.salary AS employee_salary,
        p.email,
        p.job_title
    FROM enriched a
    LEFT JOIN employees p
        ON a.manager_id = p.id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        full_name,
        manager_id,
        salary,
        level,
        is_manager,
        job_title_length,
        cumulative_job_title_length,
        salary_bucket,
        job_title_length_cume_dist,
        employee_salary,
        email,
        job_title,
        AVG(salary) OVER (PARTITION BY level, manager_id ORDER BY job_title_length ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) salary_avg_6,
        AVG(salary_bucket) OVER (PARTITION BY level ORDER BY job_title_length_cume_dist ASC, id ASC ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) salary_bucket_avg_6_prior,
        DENSE_RANK() OVER (PARTITION BY level ORDER BY employee_salary DESC NULLS FIRST) employee_salary_dense_rank,
        DENSE_RANK() OVER (PARTITION BY level, manager_id ORDER BY cumulative_job_title_length ASC NULLS LAST) cumulative_job_title_length_dense_rank
    FROM joined
),
by_type AS (
    -- Native SQL PIVOT
    SELECT *
    FROM (
        SELECT
            level,
            job_title,
            salary_bucket
        FROM metrics
    ) src
    PIVOT (
        MAX(salary_bucket) AS total,
        COUNT(1) AS cnt
        FOR job_title IN ('Engineer' AS engineer, 'Analyst' AS analyst)
    )
)
SELECT
    level,
    analyst_total,
    analyst_cnt,
    CASE WHEN level RLIKE '^(L5|L1)$' THEN 'CORE' ELSE 'GROUP_A' END AS level_zone
FROM by_type
ORDER BY analyst_cnt ASC, level_zone ASC;
