WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        log_ts,
        level,
        latency_ms,
        k,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS k, value
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        level,
        latency_ms,
        k,
        MIN(latency_ms) OVER (PARTITION BY service) AS service_min_latency_ms,
        ROW_NUMBER() OVER (PARTITION BY k ORDER BY latency_ms, id, k) AS seq_no
    FROM flattened
    WHERE k = 'trace'
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        latency_ms,
        k,
        ROUND(seq_no / SUM(seq_no) OVER (PARTITION BY level), 4) AS seq_no_share,
        DENSE_RANK() OVER w_running AS latency_ms_dense_rank,
        SUM(seq_no) OVER (ORDER BY service_min_latency_ms, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_seq_no
    FROM scored
    WINDOW w_running AS (PARTITION BY k ORDER BY latency_ms)
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, seq_no_share + latency_ms_dense_rank AS seq_no_share_plus_latency_ms_dense_rank
    FROM with_windows
    WHERE latency_ms < 818
),
cleaned_app_logs AS (
    -- Keep recent, valid rows
    SELECT
        *,
        seq_no_share_plus_latency_ms_dense_rank * latency_ms_dense_rank AS seq_no_share_plus_latency_ms_dense_rank_times_latency_ms_dense_rank,
        ROUND(seq_no_share * 1.15, 2) AS adjusted_seq_no_share
    FROM cleaned
    WHERE k IS NOT NULL
)
-- Grouped metrics
SELECT
    k,
    APPROX_PERCENTILE(seq_no_share_plus_latency_ms_dense_rank, 0.9) AS seq_no_share_plus_latency_ms_dense_rank_p90,
    COUNT_IF(k = 'region') AS region_events
FROM cleaned_app_logs
GROUP BY k
HAVING APPROX_PERCENTILE(seq_no_share_plus_latency_ms_dense_rank, 0.9) > 0
ORDER BY seq_no_share_plus_latency_ms_dense_rank_p90;
