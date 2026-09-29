WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        attr_key,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS attr_key, v
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        service,
        log_ts,
        level,
        latency_ms,
        status_code,
        attr_key
    FROM (
        SELECT *, RANK() OVER (PARTITION BY service ORDER BY latency_ms) AS dedup_rank
        FROM lines
    ) ranked
    WHERE dedup_rank = 1
),
deduped_app_logs AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, RANK() OVER (PARTITION BY service, attr_key ORDER BY latency_ms DESC) AS row_num
    FROM deduped
    QUALIFY row_num = 1
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id, service
    FROM deduped_app_logs
    EXCEPT ALL
    SELECT id, service
    FROM app_logs
    WHERE service IN ('checkout', 'search')
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM deduped_app_logs s
    JOIN remaining_keys k ON s.id = k.id AND s.service = k.service
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        service,
        level,
        latency_ms,
        status_code,
        row_num,
        CORR(status_code, latency_ms) OVER w_running AS corr_status_code_latency_ms,
        CORR(latency_ms, status_code) OVER w_running AS corr_latency_ms_status_code
    FROM reconciled
    WINDOW w_running AS (PARTITION BY attr_key)
)
SELECT
    id,
    level,
    latency_ms,
    status_code,
    row_num,
    corr_status_code_latency_ms
FROM with_windows
ORDER BY latency_ms DESC NULLS FIRST
