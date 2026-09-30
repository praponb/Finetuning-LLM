WITH with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY device ORDER BY started_at ASC, id ASC) AS row_seq,
        ROW_NUMBER() OVER (ORDER BY started_at DESC, id ASC) AS event_seq,
        ROW_NUMBER() OVER (PARTITION BY browser, country ORDER BY started_at DESC, id ASC) AS seq_no
    FROM sessions
    WHERE customer_id IN (15, 23, 35)
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        started_at,
        row_seq,
        event_seq
    FROM with_windows
    QUALIFY ROW_NUMBER() OVER (PARTITION BY device ORDER BY event_seq DESC, id ASC) = 1
),
with_latest AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.duration_sec
    FROM latest s
    JOIN LATERAL (
            SELECT c.duration_sec
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.duration_sec DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_latest s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events ch
            WHERE ch.session_id = s.id
              AND TO_DATE(ch.event_time) >= '2024-05-15'
        )
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        event_seq,
        COUNT(event_seq) OVER (ORDER BY CAST(started_at AS BIGINT) ASC RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS event_seq_count_30d,
        LAST_VALUE(row_seq, TRUE) OVER (ORDER BY duration_sec ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_row_seq,
        MAX(row_seq) OVER (ORDER BY CAST(started_at AS BIGINT) ASC RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS row_seq_max_7d
    FROM not_flagged
),
grouped AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN event_seq >= 8 THEN 'HIGH' ELSE 'LOW' END AS event_seq_tier,
        AVG(last_known_row_seq) AS avg_last_known_row_seq,
        COUNT(DISTINCT id) AS distinct_ids,
        ROUND(AVG(event_seq), 2) AS avg_event_seq,
        APPROX_PERCENTILE(event_seq, ARRAY(0.25, 0.5, 0.75)) AS event_seq_quartiles,
        APPROX_PERCENTILE(event_seq_count_30d, ARRAY(0.25, 0.5, 0.75)) AS event_seq_count_30d_quartiles
    FROM enriched
    GROUP BY CASE WHEN event_seq >= 8 THEN 'HIGH' ELSE 'LOW' END
)
SELECT
    event_seq_tier,
    avg_last_known_row_seq,
    avg_event_seq,
    event_seq_quartiles,
    event_seq_count_30d_quartiles
FROM grouped;
