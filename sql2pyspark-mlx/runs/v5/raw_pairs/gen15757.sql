WITH base AS (
    SELECT
        id,
        customer_id,
        device,
        browser,
        started_at,
        CASE WHEN browser IN ('Safari', 'Edge') THEN 'Priority' ELSE 'Other' END AS browser_group
    FROM sessions
    WHERE browser <> 'Edge'
),
not_flagged AS (
    SELECT s.*
    FROM base s
    WHERE EXISTS (
            SELECT 1
            FROM web_events x
            WHERE x.session_id = s.id
              AND x.event_type = 'purchase'
        )
),
with_windows AS (
    SELECT
        id,
        device,
        browser,
        ROW_NUMBER() OVER (PARTITION BY browser ORDER BY started_at DESC, id) AS row_seq,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY started_at DESC, id) AS row_seq_2,
        FIRST_VALUE(browser, TRUE) OVER (PARTITION BY customer_id ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_browser,
        FIRST_VALUE(browser) IGNORE NULLS OVER (PARTITION BY browser ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_browser_2
    FROM not_flagged
    WHERE device NOT IN ('tablet', 'desktop')
),
scored AS (
    SELECT
        id,
        browser,
        row_seq,
        first_known_browser_2,
        LAST_VALUE(browser) IGNORE NULLS OVER (PARTITION BY browser ORDER BY row_seq_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_browser,
        MIN(row_seq_2) OVER (PARTITION BY device) AS device_min_row_seq_2
    FROM with_windows
),
not_flagged_sessions AS (
    SELECT s.*
    FROM scored s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events x
            WHERE x.session_id = s.id
              AND x.event_type IN ('purchase', 'add_to_cart')
        )
),
remaining_keys AS (
    SELECT id
    FROM not_flagged_sessions
    INTERSECT ALL
    SELECT id
    FROM sessions
    WHERE device = 'tablet'
),
reconciled AS (
    SELECT s.*
    FROM not_flagged_sessions s
    INNER JOIN remaining_keys k ON s.id = k.id
),
base_sessions AS (
    SELECT *, ROUND(row_seq, 1) AS row_seq_rounded
    FROM reconciled
    WHERE browser IS NOT NULL
)
SELECT
    CASE WHEN device_min_row_seq_2 >= 6 THEN 'PREMIUM' ELSE 'STANDARD' END AS device_min_row_seq_2_tier,
    COUNT(*) AS num_rows,
    COUNT(DISTINCT id) AS distinct_ids
FROM base_sessions
GROUP BY CASE WHEN device_min_row_seq_2 >= 6 THEN 'PREMIUM' ELSE 'STANDARD' END
HAVING num_rows >= 1;
