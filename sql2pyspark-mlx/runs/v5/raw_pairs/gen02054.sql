WITH enriched AS (
    -- Attach accounts attributes
    SELECT
        s.id,
        s.account_id,
        s.txn_time,
        s.merchant,
        p.opened_date,
        p.status
    FROM transactions s
    LEFT JOIN accounts p
        ON s.account_id = p.id
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAST_VALUE(status, TRUE) OVER (ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_status,
        ROW_NUMBER() OVER (PARTITION BY merchant ORDER BY txn_time, id) AS event_seq,
        FIRST_VALUE(status, TRUE) OVER (PARTITION BY merchant, status ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_status
    FROM enriched
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM with_windows
    QUALIFY ROW_NUMBER() OVER (PARTITION BY status ORDER BY txn_time DESC, id) = 1
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        account_id,
        merchant,
        opened_date,
        status,
        event_seq,
        first_known_status,
        ROUND(event_seq, 2) AS event_seq_rounded,
        COALESCE(status, 'DIRECT') AS resolved_status
    FROM deduped
    WHERE txn_time > TIMESTAMP '2023-08-28 23:00:00'
),
latest AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        account_id,
        merchant,
        status,
        event_seq,
        first_known_status,
        event_seq_rounded,
        resolved_status
    FROM base
    QUALIFY ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY event_seq_rounded DESC, id) = 1
)
-- Multi-level aggregation
SELECT
    status,
    CASE WHEN event_seq_rounded >= 64 THEN 'HIGH' ELSE 'LOW' END AS event_seq_rounded_tier,
    MIN(event_seq) AS min_event_seq,
    ROUND(AVG(event_seq_rounded), 2) AS avg_event_seq_rounded,
    APPROX_PERCENTILE(event_seq, 0.95) AS event_seq_p95,
    GROUPING(status) AS gid
FROM latest
GROUP BY ROLLUP (status, CASE WHEN event_seq_rounded >= 64 THEN 'HIGH' ELSE 'LOW' END)
ORDER BY event_seq_rounded_tier;
