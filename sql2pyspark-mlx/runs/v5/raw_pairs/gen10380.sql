WITH exploded AS (
    SELECT
        id,
        host,
        level,
        latency_ms,
        k,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) x AS k, value
),
deduped AS (
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY k ORDER BY latency_ms NULLS LAST) AS dedup_rank
        FROM exploded
    ) t
    WHERE dedup_rank = 1
),
latest AS (
    SELECT
        id,
        latency_ms,
        k,
        dedup_rank
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY host ORDER BY dedup_rank, id, k) AS rank_in_group
        FROM deduped
    ) ranked
    WHERE rank_in_group = 1
),
valid AS (
    SELECT
        *,
        dedup_rank * latency_ms AS dedup_rank_times_latency_ms,
        COALESCE(k, 'N/A') AS k_clean
    FROM latest
    WHERE k IS NOT NULL
      AND k <> 'region'
)
SELECT
    CASE WHEN dedup_rank_times_latency_ms >= 71 THEN 'HIGH' ELSE 'LOW' END AS dedup_rank_times_latency_ms_tier,
    k,
    COUNT_IF(k = 'trace') AS trace_events,
    SUM(CASE WHEN k = 'trace' THEN dedup_rank_times_latency_ms ELSE 0 END) AS trace_dedup_rank_times_latency_ms,
    BIT_AND(latency_ms) AS latency_ms_bit_and,
    APPROX_PERCENTILE(latency_ms, ARRAY(0.25, 0.5, 0.75)) AS latency_ms_quartiles,
    BIT_OR(latency_ms) AS latency_ms_bit_or,
    SIZE(COLLECT_SET(k)) AS num_ks
FROM valid
GROUP BY CASE WHEN dedup_rank_times_latency_ms >= 71 THEN 'HIGH' ELSE 'LOW' END, k
HAVING trace_dedup_rank_times_latency_ms > 0
ORDER BY latency_ms_bit_and DESC NULLS LAST
