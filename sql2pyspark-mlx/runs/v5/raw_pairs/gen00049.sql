WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        log_ts,
        level,
        latency_ms,
        status_code,
        attr_key,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) t AS attr_key, v
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        attr_key,
        AVG(status_code) OVER (PARTITION BY level, attr_key ORDER BY log_ts, id, attr_key ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS status_code_avg_2,
        PERCENT_RANK() OVER (ORDER BY latency_ms DESC NULLS LAST) AS latency_ms_percent_rank
    FROM lines
    WHERE latency_ms > 1737
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        MAX(status_code_avg_2) OVER w_ordered AS last4_max_status_code_avg_2,
        AVG(status_code_avg_2) OVER w_ordered AS last4_avg_status_code_avg_2,
        MIN(status_code_avg_2) OVER w_ordered AS last4_min_status_code_avg_2,
        SUM(status_code_avg_2) OVER w_ordered AS last4_sum_status_code_avg_2
    FROM with_windows
    WINDOW w_ordered AS (PARTITION BY attr_key ORDER BY latency_ms_percent_rank, id, attr_key ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        attr_key,
        last4_max_status_code_avg_2,
        last4_avg_status_code_avg_2,
        STDDEV_SAMP(last4_min_status_code_avg_2) OVER (PARTITION BY attr_key) AS attr_key_stddev_samp_last4_min_status_code_avg_2,
        ROW_NUMBER() OVER w_part AS seq_no,
        ROUND(last4_min_status_code_avg_2 / SUM(last4_min_status_code_avg_2) OVER (PARTITION BY attr_key), 4) AS last4_min_status_code_avg_2_share
    FROM enriched
    WINDOW w_part AS (PARTITION BY attr_key ORDER BY last4_sum_status_code_avg_2 DESC, id, attr_key)
),
daily AS (
    -- Grouped metrics
    SELECT
        attr_key,
        seq_no,
        COUNT(CASE WHEN attr_key = 'trace' THEN 1 END) AS trace_count,
        ROUND(AVG(seq_no), 2) AS avg_seq_no,
        ROUND(AVG(last4_min_status_code_avg_2_share), 2) AS avg_last4_min_status_code_avg_2_share,
        COUNT_IF(attr_key = 'region') AS region_events,
        SIZE(COLLECT_SET(attr_key)) AS num_attr_keys,
        GROUPING_ID() AS grouping_level
    FROM scored
    GROUP BY GROUPING SETS ((attr_key, seq_no), (attr_key), ())
    HAVING ROUND(AVG(seq_no), 2) < 1000000
)
SELECT
    seq_no,
    trace_count,
    avg_last4_min_status_code_avg_2_share,
    region_events,
    num_attr_keys
FROM daily
ORDER BY avg_last4_min_status_code_avg_2_share DESC, trace_count
