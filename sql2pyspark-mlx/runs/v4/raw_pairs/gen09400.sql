WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        k,
        attr_value
    FROM carts
    LATERAL VIEW EXPLODE(attributes) e AS k, attr_value
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY k ORDER BY created_at DESC) AS dedup_rank
        FROM lines
    ) x
    WHERE dedup_rank = 1
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY k ORDER BY created_at DESC, id, k) AS row_seq,
        FIRST_VALUE(k, TRUE) OVER w_ordered AS first_known_k
    FROM latest
    WINDOW w_ordered AS (PARTITION BY k ORDER BY created_at, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
kept_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM with_windows
    INTERSECT
    SELECT id
    FROM carts
    WHERE created_at < '2025-02-01'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_windows s
    INNER JOIN kept_keys k ON s.id = k.id
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        ROUND(row_seq / SUM(row_seq) OVER (PARTITION BY k), 4) AS row_seq_share,
        ROUND(row_seq / SUM(row_seq) OVER (PARTITION BY k), 4) AS row_seq_share_2,
        DENSE_RANK() OVER (PARTITION BY k ORDER BY row_seq DESC NULLS FIRST) AS row_seq_dense_rank
    FROM reconciled
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('HOUR', created_at) AS hour_window,
        DATE_TRUNC('DAY', created_at) AS activity_day,
        APPROX_PERCENTILE(row_seq_share_2, ARRAY(0.25, 0.5, 0.75)) AS row_seq_share_2_quartiles,
        COUNT_IF(k = 'utm_source') AS utm_source_events,
        RANK() OVER (PARTITION BY DATE_TRUNC('HOUR', created_at) ORDER BY COUNT_IF(k = 'utm_source') DESC) AS utm_source_events_rank
    FROM enriched
    GROUP BY hour_window, activity_day
)
SELECT
    hour_window,
    activity_day,
    utm_source_events,
    utm_source_events_rank
FROM summary;
