WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        email,
        manager_id,
        job_title,
        salary,
        level,
        CONCAT(email, '/', level) AS email_level
    FROM employees
    WHERE job_title IS NOT NULL
      AND job_title IS DISTINCT FROM 'Analyst'
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY job_title ORDER BY salary DESC, id) AS rank_in_group
        FROM valid
    ) t
    WHERE rank_in_group = 1
),
valid_employees AS (
    -- Keep recent, valid rows
    SELECT *, ROUND(rank_in_group, 1) AS rank_in_group_rounded
    FROM latest
    WHERE job_title <> 'Intern'
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        email,
        manager_id,
        job_title,
        salary,
        rank_in_group,
        rank_in_group_rounded,
        ROUND(salary * 1.1, 2) AS salary_scaled,
        COALESCE(level, 'N/A') AS resolved_level
    FROM valid_employees
    WHERE job_title = 'Designer'
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        salary - rank_in_group AS salary_minus_rank_in_group,
        LOWER(email) AS email_lower
    FROM base
    WHERE salary_scaled > 74
),
latest_employees AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM filtered
    QUALIFY ROW_NUMBER() OVER (PARTITION BY manager_id ORDER BY salary DESC, id) = 1
),
totals AS (
    -- Grouped metrics
    SELECT
        CASE WHEN rank_in_group_rounded >= 75 THEN 'HIGH' ELSE 'LOW' END AS rank_in_group_rounded_tier,
        MAX(rank_in_group_rounded) FILTER (WHERE job_title = 'Intern') AS intern_max_rank_in_group_rounded,
        PERCENTILE_APPROX(salary_scaled, ARRAY(0.25, 0.5, 0.75)) AS salary_scaled_quartiles,
        COUNT_IF(job_title = 'Designer') AS designer_events,
        COUNT(CASE WHEN job_title = 'Director' THEN 1 END) AS director_count,
        SORT_ARRAY(COLLECT_SET(job_title)) AS job_title_list,
        SUM(CASE WHEN job_title = 'Engineer' THEN rank_in_group_rounded ELSE 0 END) AS engineer_rank_in_group_rounded
    FROM latest_employees
    GROUP BY CASE WHEN rank_in_group_rounded >= 75 THEN 'HIGH' ELSE 'LOW' END
)
SELECT
    rank_in_group_rounded_tier,
    intern_max_rank_in_group_rounded,
    salary_scaled_quartiles,
    designer_events,
    job_title_list,
    engineer_rank_in_group_rounded
FROM totals
ORDER BY designer_events NULLS LAST;
