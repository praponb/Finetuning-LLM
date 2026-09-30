WITH with_windows AS (
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
        MAX(salary) OVER (PARTITION BY job_title) AS job_title_max_salary,
        LAST_VALUE(level, TRUE) OVER (ORDER BY salary ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_level
    FROM employees
),
with_windows_employees AS (
    SELECT
        *,
        LAST_VALUE(job_title, TRUE) OVER (PARTITION BY level ORDER BY salary ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_job_title,
        ROUND(salary / SUM(salary) OVER (PARTITION BY job_title), 4) AS salary_share,
        AVG(job_title_max_salary) OVER (PARTITION BY dept_id ORDER BY hire_date ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS job_title_max_salary_avg_3,
        ROW_NUMBER() OVER w_grp AS seq_no
    FROM with_windows
    WINDOW w_grp AS (PARTITION BY manager_id ORDER BY job_title_max_salary DESC, id ASC)
),
filtered AS (
    SELECT *, CASE WHEN level IN ('L3', 'L5') THEN 'Priority' ELSE 'Other' END AS level_group
    FROM with_windows_employees
    WHERE hire_date >= DATE '2023-06-02'
      AND manager_id IS NOT NULL
),
enriched AS (
    SELECT
        *,
        AVG(salary_share) OVER w_recent AS salary_share_avg_3_prior,
        SUM(job_title_max_salary_avg_3) OVER w_part AS cumulative_job_title_max_salary_avg_3
    FROM filtered
    WINDOW w_recent AS (ORDER BY salary ASC, id ASC ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), w_part AS (PARTITION BY level ORDER BY job_title_max_salary_avg_3 ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    CASE WHEN seq_no >= 9 THEN 'PREMIUM' ELSE 'STANDARD' END AS seq_no_tier,
    CASE WHEN job_title_max_salary_avg_3 >= 146540 THEN 'LARGE' ELSE 'SMALL' END AS job_title_max_salary_avg_3_tier,
    ROUND(SUM(salary_share) / NULLIF(COUNT(CASE WHEN level = 'L3' THEN 1 END), 0), 2) AS salary_share_per_l3,
    ROUND(AVG(salary), 2) AS avg_salary,
    APPROX_PERCENTILE(cumulative_job_title_max_salary_avg_3, 0.5) AS cumulative_job_title_max_salary_avg_3_p50,
    GROUPING_ID() AS gid
FROM enriched
GROUP BY ROLLUP (CASE WHEN seq_no >= 9 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN job_title_max_salary_avg_3 >= 146540 THEN 'LARGE' ELSE 'SMALL' END)
ORDER BY seq_no_tier ASC
