WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        job_title,
        salary,
        hire_date,
        is_remote,
        level,
        DATEDIFF(CURRENT_DATE(), hire_date) AS days_since_hire_date,
        CAST(is_remote AS INT) AS is_remote_flag,
        COALESCE(level, 'DIRECT') AS level_clean
    FROM employees
    WHERE hire_date >= DATE '2023-06-02'
      AND manager_id IS NOT NULL
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        job_title,
        salary,
        hire_date,
        is_remote,
        days_since_hire_date,
        level_clean,
        AVG(is_remote_flag) OVER (ORDER BY salary ASC, id ASC ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS is_remote_flag_avg_3_prior,
        SUM(days_since_hire_date) OVER w_hist AS running_days_since_hire_date,
        SUM(days_since_hire_date) OVER (PARTITION BY job_title ORDER BY is_remote_flag ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_days_since_hire_date
    FROM filtered
    WINDOW w_hist AS (PARTITION BY level ORDER BY salary ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        salary,
        is_remote,
        cumulative_days_since_hire_date,
        AVG(cumulative_days_since_hire_date) OVER (PARTITION BY job_title ORDER BY days_since_hire_date ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_avg_cumulative_days_since_hire_date,
        MIN(cumulative_days_since_hire_date) OVER (PARTITION BY job_title ORDER BY days_since_hire_date ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_min_cumulative_days_since_hire_date,
        SUM(cumulative_days_since_hire_date) OVER (PARTITION BY job_title ORDER BY days_since_hire_date ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_sum_cumulative_days_since_hire_date
    FROM metrics
),
valid AS (
    -- Basic cleansing and defaults
    SELECT *, salary * cumulative_days_since_hire_date AS salary_times_cumulative_days_since_hire_date
    FROM enriched
    WHERE cumulative_days_since_hire_date >= 187
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN salary_times_cumulative_days_since_hire_date >= 80 THEN 'HIGH' ELSE 'LOW' END AS salary_times_cumulative_days_since_hire_date_tier,
    CASE WHEN last4_min_cumulative_days_since_hire_date >= 440 THEN 'LARGE' ELSE 'SMALL' END AS last4_min_cumulative_days_since_hire_date_tier,
    COUNT(DISTINCT id) AS distinct_ids,
    APPROX_PERCENTILE(last4_sum_cumulative_days_since_hire_date, 0.5) AS last4_sum_cumulative_days_since_hire_date_p50,
    APPROX_PERCENTILE(last4_sum_cumulative_days_since_hire_date, 0.9) AS last4_sum_cumulative_days_since_hire_date_p90
FROM valid
GROUP BY CUBE (CASE WHEN salary_times_cumulative_days_since_hire_date >= 80 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN last4_min_cumulative_days_since_hire_date >= 440 THEN 'LARGE' ELSE 'SMALL' END)
ORDER BY salary_times_cumulative_days_since_hire_date_tier ASC NULLS LAST
