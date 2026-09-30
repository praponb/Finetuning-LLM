WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        latency_ms,
        attr_key,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) t AS attr_key, v
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, INITCAP(TRIM(service)) AS service_title
    FROM lines
    WHERE attr_key IN ('region', 'version')
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        FLOOR(latency_ms) AS latency_ms_whole,
        COALESCE(attr_key, 'OTHER') AS attr_key_clean
    FROM filtered
    WHERE latency_ms BETWEEN 874 AND 1853
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        service,
        host,
        latency_ms,
        attr_key,
        v,
        service_title,
        latency_ms_whole,
        attr_key_clean,
        LAST_VALUE(latency_ms) IGNORE NULLS OVER w_ordered AS last_known_latency_ms,
        SUM(latency_ms) OVER (PARTITION BY host ORDER BY latency_ms, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_latency_ms,
        DENSE_RANK() OVER (PARTITION BY service ORDER BY latency_ms_whole DESC NULLS LAST) AS latency_ms_whole_dense_rank,
        ROUND(latency_ms / SUM(latency_ms) OVER (PARTITION BY service), 4) AS latency_ms_share
    FROM cleaned
    WINDOW w_ordered AS (PARTITION BY attr_key ORDER BY latency_ms_whole, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
-- Running and moving aggregates
SELECT
    *,
    ROUND(cumulative_latency_ms / SUM(cumulative_latency_ms) OVER (PARTITION BY attr_key, service), 4) AS cumulative_latency_ms_share,
    AVG(cumulative_latency_ms) OVER (PARTITION BY attr_key ORDER BY last_known_latency_ms, id, attr_key ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS cumulative_latency_ms_avg_6
FROM enriched;
