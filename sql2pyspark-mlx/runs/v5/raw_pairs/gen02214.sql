WITH deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT id, device
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY browser, country ORDER BY started_at ASC NULLS FIRST, id ASC) AS rank_in_group
        FROM sessions
    ) t
    WHERE rank_in_group = 1
),
kept_keys AS (
    -- Set intersection on keys
    SELECT device, id
    FROM deduped
    INTERSECT
    SELECT device, id
    FROM sessions
    WHERE browser IN ('Firefox', 'Chrome', 'Edge')
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM deduped s
    JOIN kept_keys k ON s.device = k.device AND s.id = k.id
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM kept_rows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events ch
            WHERE ch.session_id = s.id
              AND ch.session_id IN (7, 13, 25)
        )
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM not_flagged s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.session_id IN (1, 14, 21)
        )
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM kept s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events ch
            WHERE ch.session_id = s.id
              AND ch.page IS NOT NULL
        )
),
matched_keys AS (
    -- Set difference on keys
    SELECT id, device
    FROM eligible
    EXCEPT
    SELECT id, device
    FROM sessions
    WHERE customer_id IN (37, 39)
),
kept_rows_sessions AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM eligible s
    JOIN matched_keys k ON s.id = k.id AND s.device = k.device
)
-- Multi-level aggregation
SELECT
    device,
    COUNT(*) FILTER (WHERE device = 'desktop') AS desktop_rows,
    SIZE(COLLECT_SET(device)) AS num_devices,
    COUNT(*) FILTER (WHERE device = 'desktop') AS desktop_rows_2,
    COUNT(CASE WHEN device = 'desktop' THEN 1 END) AS desktop_count
FROM kept_rows_sessions
GROUP BY 1
ORDER BY num_devices DESC
