WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        level,
        latency_ms,
        attr_key,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) t AS attr_key, v
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        LAST_VALUE(latency_ms) IGNORE NULLS OVER w_ordered AS last_known_latency_ms,
        DENSE_RANK() OVER (PARTITION BY host, attr_key ORDER BY latency_ms DESC NULLS FIRST) AS latency_ms_dense_rank
    FROM lines
    WINDOW w_ordered AS (PARTITION BY attr_key ORDER BY latency_ms, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(latency_ms) OVER w_grp AS prev_latency_ms,
        LEAD(latency_ms_dense_rank) OVER w_grp AS next_latency_ms_dense_rank,
        LAG(latency_ms_dense_rank) OVER w_grp AS prev_latency_ms_dense_rank,
        ROW_NUMBER() OVER w_grp AS seq_no,
        NTILE(5) OVER (PARTITION BY host ORDER BY last_known_latency_ms DESC, id, attr_key) AS last_known_latency_ms_bucket
    FROM metrics
    WINDOW w_grp AS (PARTITION BY level ORDER BY last_known_latency_ms, id, attr_key)
),
metrics_app_logs AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY last_known_latency_ms, id, attr_key) AS seq_no_2,
        AVG(latency_ms_dense_rank) OVER (PARTITION BY level ORDER BY last_known_latency_ms, id, attr_key ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS latency_ms_dense_rank_avg_3,
        ROW_NUMBER() OVER (ORDER BY last_known_latency_ms_bucket, id, attr_key) AS row_seq
    FROM scored
),
scored_app_logs AS (
    -- Running and moving aggregates
    SELECT
        id,
        level,
        latency_ms,
        attr_key,
        latency_ms_dense_rank,
        prev_latency_ms_dense_rank,
        seq_no,
        seq_no_2,
        row_seq,
        ROUND(latency_ms_dense_rank_avg_3 / SUM(latency_ms_dense_rank_avg_3) OVER (PARTITION BY host, attr_key), 4) AS latency_ms_dense_rank_avg_3_share,
        AVG(latency_ms_dense_rank) OVER (PARTITION BY level ORDER BY seq_no, id, attr_key ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS latency_ms_dense_rank_avg_5_prior,
        NTILE(4) OVER (ORDER BY seq_no_2, id, attr_key) AS seq_no_2_bucket
    FROM metrics_app_logs
)
SELECT
    id,
    attr_key,
    latency_ms_dense_rank,
    seq_no,
    seq_no_2,
    row_seq,
    latency_ms_dense_rank_avg_3_share,
    latency_ms_dense_rank_avg_5_prior,
    seq_no_2_bucket
FROM scored_app_logs
