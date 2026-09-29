WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        log_ts,
        level,
        latency_ms,
        status_code,
        k,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) x AS k, value
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(status_code) OVER (PARTITION BY service ORDER BY log_ts ASC, id ASC, k ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS status_code_avg_3,
        ROW_NUMBER() OVER (PARTITION BY level ORDER BY log_ts DESC, id ASC, k ASC) AS row_seq
    FROM exploded
    WHERE service NOT IN ('search', 'auth')
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM enriched
    QUALIFY RANK() OVER (PARTITION BY level, k ORDER BY status_code DESC) = 1
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM ranked
    QUALIFY ROW_NUMBER() OVER (PARTITION BY k ORDER BY status_code DESC, id ASC, k ASC) = 1
)
SELECT
    id,
    service,
    latency_ms,
    status_code,
    k,
    value,
    status_code_avg_3,
    CASE WHEN k RLIKE '^(version|trace)$' THEN 'CORE' WHEN k RLIKE '^(region)$' THEN 'OTHER_ZONE' ELSE 'EXTENDED' END AS k_zone
FROM deduped;
