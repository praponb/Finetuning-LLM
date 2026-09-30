WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        latency_ms,
        status_code,
        attr_key,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS attr_key, value
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, RANK() OVER (PARTITION BY attr_key ORDER BY status_code DESC) AS dedup_rank
    FROM flattened
    QUALIFY dedup_rank = 1
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(status_code) OVER w_hist AS prev_status_code,
        LEAD(status_code) OVER w_hist AS next_status_code,
        LAG(latency_ms) OVER w_hist AS prev_latency_ms,
        ROW_NUMBER() OVER w_hist AS seq_no,
        NTILE(4) OVER (PARTITION BY attr_key ORDER BY latency_ms, id, attr_key) AS latency_ms_bucket
    FROM deduped
    WHERE status_code = 413
    WINDOW w_hist AS (PARTITION BY attr_key ORDER BY status_code, id, attr_key)
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY attr_key ORDER BY status_code, id, attr_key) AS latest_rank
        FROM scored
    ) ranked
    WHERE latest_rank = 1
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        attr_key,
        value,
        dedup_rank,
        prev_status_code,
        prev_latency_ms,
        latest_rank,
        AVG(prev_status_code) OVER w_ordered AS last4_avg_prev_status_code,
        MIN(prev_status_code) OVER w_ordered AS last4_min_prev_status_code,
        COUNT(prev_status_code) OVER w_ordered AS last4_count_prev_status_code,
        STDDEV_SAMP(prev_status_code) OVER w_running AS attr_key_stddev_samp_prev_status_code
    FROM latest
    WINDOW w_ordered AS (PARTITION BY attr_key ORDER BY latest_rank, id, attr_key ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), w_running AS (PARTITION BY attr_key)
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        attr_key,
        value,
        dedup_rank,
        prev_status_code,
        latest_rank,
        last4_avg_prev_status_code,
        last4_min_prev_status_code,
        last4_count_prev_status_code,
        attr_key_stddev_samp_prev_status_code,
        latest_rank * attr_key_stddev_samp_prev_status_code AS latest_rank_times_attr_key_stddev_samp_prev_status_code
    FROM enriched
    WHERE attr_key IS NOT NULL
      AND prev_status_code != 294
),
enriched_app_logs AS (
    -- Running and moving aggregates
    SELECT
        id,
        attr_key,
        latest_rank,
        last4_avg_prev_status_code,
        attr_key_stddev_samp_prev_status_code,
        LAG(latest_rank_times_attr_key_stddev_samp_prev_status_code) OVER (PARTITION BY attr_key ORDER BY prev_status_code, id, attr_key) AS prev_latest_rank_times_attr_key_stddev_samp_prev_status_code,
        LAG(last4_avg_prev_status_code) OVER (PARTITION BY attr_key ORDER BY prev_status_code, id, attr_key) AS prev_last4_avg_prev_status_code,
        ROW_NUMBER() OVER (PARTITION BY attr_key ORDER BY prev_status_code, id, attr_key) AS row_seq,
        LEAD(attr_key_stddev_samp_prev_status_code, 1) OVER (ORDER BY latest_rank_times_attr_key_stddev_samp_prev_status_code, id, attr_key) AS next_attr_key_stddev_samp_prev_status_code
    FROM base
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN next_attr_key_stddev_samp_prev_status_code >= 426 THEN 'PREMIUM' ELSE 'STANDARD' END AS next_attr_key_stddev_samp_prev_status_code_tier,
        COUNT(*) FILTER (WHERE attr_key = 'trace') AS trace_rows,
        ROUND(SUM(next_attr_key_stddev_samp_prev_status_code) / NULLIF(COUNT(CASE WHEN attr_key = 'trace' THEN 1 END), 0), 2) AS next_attr_key_stddev_samp_prev_status_code_per_trace,
        ROUND(SUM(attr_key_stddev_samp_prev_status_code) / NULLIF(COUNT(CASE WHEN attr_key = 'trace' THEN 1 END), 0), 2) AS attr_key_stddev_samp_prev_status_code_per_trace,
        SUM(CASE WHEN attr_key = 'trace' THEN next_attr_key_stddev_samp_prev_status_code ELSE 0 END) AS trace_next_attr_key_stddev_samp_prev_status_code,
        COUNT(*) AS num_rows,
        COUNT_IF(attr_key = 'region') AS region_events
    FROM enriched_app_logs
    GROUP BY CASE WHEN next_attr_key_stddev_samp_prev_status_code >= 426 THEN 'PREMIUM' ELSE 'STANDARD' END
)
SELECT
    next_attr_key_stddev_samp_prev_status_code_tier,
    trace_rows,
    attr_key_stddev_samp_prev_status_code_per_trace,
    trace_next_attr_key_stddev_samp_prev_status_code,
    num_rows
FROM grouped
ORDER BY num_rows DESC NULLS FIRST
