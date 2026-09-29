WITH filtered AS (
    -- Keep recent, valid rows
    SELECT *, DATEDIFF(CURRENT_DATE(), hire_date) days_since_hire_date
    FROM employees
    WHERE hire_date >= TO_DATE('2023-04-27')
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY dept_id ORDER BY days_since_hire_date DESC NULLS LAST) AS latest_rank
        FROM filtered
    ) x
    WHERE latest_rank = 1
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        hire_date,
        level,
        latest_rank,
        MAX(days_since_hire_date) OVER w_part running_max_days_since_hire_date,
        COUNT(days_since_hire_date) OVER w_part running_count_days_since_hire_date,
        SUM(days_since_hire_date) OVER w_part running_sum_days_since_hire_date,
        AVG(days_since_hire_date) OVER w_part running_avg_days_since_hire_date,
        RANK() OVER (ORDER BY days_since_hire_date NULLS FIRST) days_since_hire_date_rank
    FROM ranked
    WHERE salary BETWEEN 104000 AND 125000
    WINDOW w_part AS (PARTITION BY job_title ORDER BY latest_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, CEIL(running_sum_days_since_hire_date) running_sum_days_since_hire_date_whole
    FROM with_windows
    WHERE latest_rank >= 1
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        level,
        DATE_TRUNC('YEAR', hire_date) AS year_start,
        COUNT(CASE WHEN level = 'L2' THEN 1 END) AS l2_count,
        ROUND(SUM(running_avg_days_since_hire_date) / NULLIF(COUNT(CASE WHEN level = 'L3' THEN 1 END), 0), 2) AS running_avg_days_since_hire_date_per_l3,
        GROUPING_ID() AS gid
    FROM cleaned
    GROUP BY GROUPING SETS ((level, DATE_TRUNC('YEAR', hire_date)), (level), ())
    HAVING l2_count > 1
)
SELECT
    l2_count,
    running_avg_days_since_hire_date_per_l3,
    gid
FROM summary
ORDER BY l2_count DESC NULLS LAST
