WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        log_ts,
        latency_ms,
        key,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) x AS key, attr_value
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY service, key ORDER BY latency_ms DESC NULLS FIRST, id, key) AS row_num
        FROM lines
    ) t
    WHERE row_num = 1
),
deduped_app_logs AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY service, host ORDER BY latency_ms NULLS FIRST, id, key) AS row_num_2
        FROM deduped
    ) x
    WHERE row_num_2 = 1
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM deduped_app_logs
    QUALIFY ROW_NUMBER() OVER (PARTITION BY service ORDER BY row_num_2, id, key) = 1
)
SELECT
    id,
    service,
    host,
    latency_ms,
    key,
    attr_value,
    row_num_2
FROM latest
ORDER BY service DESC NULLS LAST;
