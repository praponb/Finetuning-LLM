WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        country,
        CASE WHEN device = 'desktop' THEN 1 ELSE 0 END AS is_desktop,
        COALESCE(browser, 'NONE') AS resolved_browser
    FROM sessions
    WHERE country IS DISTINCT FROM 'US'
      AND customer_id IN (2, 26, 28)
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY country ORDER BY is_desktop DESC) AS dedup_rank
        FROM filtered
    ) ranked
    WHERE dedup_rank = 1
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(is_desktop) OVER (PARTITION BY country ORDER BY dedup_rank, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_avg_is_desktop,
        SUM(is_desktop) OVER (PARTITION BY country ORDER BY dedup_rank, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_sum_is_desktop,
        MAX(is_desktop) OVER (PARTITION BY country ORDER BY dedup_rank, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_max_is_desktop,
        LAST_VALUE(is_desktop) IGNORE NULLS OVER (PARTITION BY country ORDER BY dedup_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_is_desktop
    FROM deduped
    WHERE dedup_rank = 1
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM scored s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.session_id IN (27, 32, 36)
        )
)
SELECT
    id,
    country,
    resolved_browser,
    dedup_rank,
    last7_sum_is_desktop,
    last7_max_is_desktop
FROM kept
ORDER BY dedup_rank DESC, last7_sum_is_desktop DESC;
