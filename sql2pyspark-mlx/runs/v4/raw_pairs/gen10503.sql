WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        log_ts,
        latency_ms,
        status_code,
        key,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) x AS key, value
),
unpacked AS (
    -- Higher-order array functions
    SELECT
        id,
        service,
        status_code,
        key,
        value,
        CAST((status_code & 8) != 0 AS INT) AS flag_bit8,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM exploded
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(flag_bit8, 1) OVER w_part AS prev_flag_bit8,
        STDDEV_SAMP(status_code_high) OVER (PARTITION BY service) AS service_stddev_samp_status_code_high
    FROM unpacked
    WINDOW w_part AS (PARTITION BY service ORDER BY status_code_high, id, key)
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY service, key ORDER BY flag_bit8 DESC NULLS LAST, id, key) AS latest_rank
        FROM scored
    ) t
    WHERE latest_rank = 1
)
SELECT
    id,
    service,
    status_code,
    key,
    value,
    flag_bit8,
    status_code_high,
    prev_flag_bit8,
    latest_rank
FROM ranked;
