WITH latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget DESC NULLS FIRST, id) AS row_num
        FROM departments
    ) ranked
    WHERE row_num = 1
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN location = 'Tokyo' THEN 1 ELSE 0 END AS is_tokyo,
        COALESCE(location, 'DIRECT') AS location_clean
    FROM latest
    WHERE dept_name IS NOT DISTINCT FROM 'Engineering'
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *, RANK() OVER (PARTITION BY location ORDER BY row_num DESC) AS latest_rank
    FROM base
    QUALIFY latest_rank = 1
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id, location
    FROM deduped
    EXCEPT
    SELECT id, location
    FROM departments
    WHERE budget <= 3580000
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM deduped s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.location = k.location
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN is_tokyo >= 83 THEN 'PREMIUM' ELSE 'STANDARD' END AS is_tokyo_tier,
    SORT_ARRAY(COLLECT_SET(location)) AS location_list,
    SIZE(COLLECT_SET(dept_name)) AS num_dept_names,
    PERCENTILE_APPROX(is_tokyo, 0.5) AS is_tokyo_p50
FROM reconciled
GROUP BY CASE WHEN is_tokyo >= 83 THEN 'PREMIUM' ELSE 'STANDARD' END
ORDER BY is_tokyo_p50 DESC;
