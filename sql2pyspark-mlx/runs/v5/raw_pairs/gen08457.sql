WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT *, LOWER(level) AS level_lower
    FROM employees
    WHERE hire_date >= DATE '2023-02-19'
      AND level IS DISTINCT FROM 'L2'
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        job_title,
        is_remote,
        level,
        SUM(salary) OVER w_running AS running_salary,
        CUME_DIST() OVER w_hist AS salary_cume_dist,
        ROUND(salary / SUM(salary) OVER (PARTITION BY manager_id), 4) AS salary_share
    FROM filtered
    WHERE level = 'L4'
    WINDOW w_running AS (ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_hist AS (PARTITION BY job_title ORDER BY salary DESC)
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY job_title ORDER BY salary_cume_dist NULLS LAST) AS latest_rank
        FROM enriched
    ) t
    WHERE latest_rank = 1
),
enriched_employees AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(salary_cume_dist) OVER (PARTITION BY job_title, level ORDER BY salary_cume_dist, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_salary_cume_dist,
        ROW_NUMBER() OVER (PARTITION BY level ORDER BY salary_share, id) AS event_seq
    FROM deduped
),
deduped_employees AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        level,
        running_salary,
        salary_cume_dist,
        salary_share,
        latest_rank,
        running_salary_cume_dist
    FROM enriched_employees
    QUALIFY ROW_NUMBER() OVER (PARTITION BY level ORDER BY salary_cume_dist DESC NULLS LAST, id) = 1
)
-- Multi-level aggregation
SELECT
    CASE WHEN running_salary >= 659662 THEN 'HIGH' ELSE 'LOW' END AS running_salary_tier,
    COUNT(CASE WHEN level = 'L4' THEN 1 END) AS l4_count,
    SORT_ARRAY(COLLECT_SET(level)) AS level_list,
    ROUND(AVG(salary_cume_dist), 2) AS avg_salary_cume_dist,
    COUNT_IF(level = 'L5') AS l5_events,
    MAX(running_salary) FILTER (WHERE level = 'L2') AS l2_max_running_salary
FROM deduped_employees
GROUP BY CASE WHEN running_salary >= 659662 THEN 'HIGH' ELSE 'LOW' END
ORDER BY l4_count DESC;
