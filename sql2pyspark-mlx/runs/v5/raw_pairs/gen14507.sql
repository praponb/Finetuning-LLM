WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        latency_ms,
        k,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS k, value
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT service
    FROM lines
    INTERSECT
    SELECT service
    FROM app_logs
    WHERE host <> 'h1'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM lines s
    INNER JOIN remaining_keys k ON s.service = k.service
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT *, CASE WHEN k IN ('trace', 'version') THEN 'Tier 1' ELSE 'Other' END AS k_group
    FROM reconciled
    WHERE k IS NOT NULL
      AND k IS DISTINCT FROM 'region'
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROUND(latency_ms / SUM(latency_ms) OVER (PARTITION BY service, k), 4) AS latency_ms_share,
        RANK() OVER (ORDER BY latency_ms DESC NULLS FIRST) AS latency_ms_rank,
        STDDEV_SAMP(latency_ms) OVER (PARTITION BY k, service) AS k_stddev_samp_latency_ms,
        RANK() OVER (PARTITION BY service ORDER BY latency_ms) AS latency_ms_rank_2,
        RANK() OVER w_ordered AS latency_ms_rank_3
    FROM filtered
    WINDOW w_ordered AS (PARTITION BY service ORDER BY latency_ms DESC NULLS LAST)
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        service,
        latency_ms,
        k,
        value,
        k_group,
        latency_ms_share,
        latency_ms_rank,
        k_stddev_samp_latency_ms,
        latency_ms_rank_2,
        ROUND(latency_ms_rank_2, 1) AS latency_ms_rank_2_rounded,
        COALESCE(k, 'DIRECT') AS k_filled
    FROM metrics
    WHERE k IS NOT NULL
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN k_stddev_samp_latency_ms >= 1930 THEN 'HIGH' ELSE 'LOW' END AS k_stddev_samp_latency_ms_tier,
    k,
    COUNT_IF(service = 'search') AS search_events,
    COUNT(*) AS num_rows,
    ROUND(AVG(latency_ms_rank_2_rounded), 2) AS avg_latency_ms_rank_2_rounded,
    COUNT_IF(k = 'version') AS version_events
FROM base
GROUP BY CASE WHEN k_stddev_samp_latency_ms >= 1930 THEN 'HIGH' ELSE 'LOW' END, k
ORDER BY k_stddev_samp_latency_ms_tier DESC NULLS LAST;
