WITH flattened AS (
    SELECT
        id,
        service,
        host,
        latency_ms,
        k,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS k, v
),
base AS (
    SELECT
        id,
        latency_ms,
        k,
        v,
        INITCAP(TRIM(service)) AS service_title,
        COALESCE(k, 'DIRECT') AS k_filled
    FROM flattened
    WHERE k IS NOT NULL
),
latest AS (
    SELECT *
    FROM base
    QUALIFY ROW_NUMBER() OVER (PARTITION BY k ORDER BY latency_ms, id, k) = 1
),
scored AS (
    SELECT
        *,
        LEAD(latency_ms, 1) OVER (PARTITION BY k ORDER BY latency_ms, id, k) AS next_latency_ms,
        LEAD(latency_ms, 1) OVER (PARTITION BY k ORDER BY latency_ms, id, k) AS next_latency_ms_2,
        FIRST_VALUE(k, TRUE) OVER (PARTITION BY k ORDER BY latency_ms, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_k
    FROM latest
),
metrics AS (
    SELECT
        *,
        AVG(next_latency_ms_2) OVER (PARTITION BY k ORDER BY next_latency_ms_2, id, k ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS next_latency_ms_2_avg_2,
        DENSE_RANK() OVER (PARTITION BY k ORDER BY latency_ms DESC NULLS FIRST) AS latency_ms_dense_rank
    FROM scored
),
ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY k ORDER BY next_latency_ms DESC NULLS FIRST, id, k) AS rank_in_group
    FROM metrics
    QUALIFY rank_in_group = 1
),
scored_app_logs AS (
    SELECT
        id,
        latency_ms,
        k,
        v,
        service_title,
        k_filled,
        next_latency_ms,
        next_latency_ms_2,
        first_known_k,
        next_latency_ms_2_avg_2,
        latency_ms_dense_rank,
        rank_in_group,
        RANK() OVER (PARTITION BY k ORDER BY latency_ms_dense_rank DESC NULLS FIRST) AS latency_ms_dense_rank_rank,
        FIRST_VALUE(latency_ms_dense_rank, TRUE) OVER w_grp AS first_known_latency_ms_dense_rank
    FROM ranked
    WINDOW w_grp AS (ORDER BY latency_ms_dense_rank, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    latency_ms,
    k,
    v,
    service_title,
    k_filled,
    next_latency_ms,
    next_latency_ms_2,
    first_known_k,
    next_latency_ms_2_avg_2,
    latency_ms_dense_rank,
    rank_in_group,
    latency_ms_dense_rank_rank,
    first_known_latency_ms_dense_rank,
    COUNT(first_known_latency_ms_dense_rank) OVER (PARTITION BY k) AS k_count_first_known_latency_ms_dense_rank,
    NTILE(4) OVER (PARTITION BY k ORDER BY latency_ms, id, k) AS latency_ms_bucket,
    RANK() OVER (PARTITION BY k ORDER BY next_latency_ms_2_avg_2 NULLS FIRST) AS next_latency_ms_2_avg_2_rank,
    LAST_VALUE(first_known_latency_ms_dense_rank, TRUE) OVER (PARTITION BY k ORDER BY latency_ms_dense_rank, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_first_known_latency_ms_dense_rank,
    AVG(latency_ms_dense_rank_rank) OVER (PARTITION BY k ORDER BY latency_ms, id, k ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS latency_ms_dense_rank_rank_avg_3
FROM scored_app_logs;
