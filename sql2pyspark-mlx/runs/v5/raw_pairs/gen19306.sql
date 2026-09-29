WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        log_ts,
        latency_ms,
        status_code,
        attr_key,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS attr_key, attr_value
),
parsed AS (
    -- Higher-order array functions
    SELECT
        service,
        latency_ms,
        status_code,
        attr_key,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        CAST((status_code & 4) != 0 AS INT) AS flag_bit4
    FROM lines
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        service,
        latency_ms,
        status_code,
        attr_key,
        flag_bit4,
        latency_ms + status_code_high AS latency_ms_plus_status_code_high
    FROM parsed
    WHERE attr_key IS NOT NULL
      AND status_code BETWEEN 334 AND 404
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        service,
        latency_ms,
        flag_bit4,
        latency_ms_plus_status_code_high,
        RANK() OVER (PARTITION BY service ORDER BY flag_bit4 NULLS LAST) AS flag_bit4_rank,
        DENSE_RANK() OVER (ORDER BY flag_bit4 DESC) AS flag_bit4_dense_rank,
        SUM(latency_ms) OVER w_part AS attr_key_sum_latency_ms,
        DENSE_RANK() OVER w_hist AS flag_bit4_dense_rank_2,
        PERCENT_RANK() OVER w_ordered AS latency_ms_plus_status_code_high_percent_rank
    FROM base
    WINDOW w_part AS (PARTITION BY attr_key), w_hist AS (PARTITION BY attr_key ORDER BY flag_bit4 DESC NULLS LAST), w_ordered AS (PARTITION BY service ORDER BY latency_ms_plus_status_code_high NULLS LAST)
)
SELECT
    service,
    flag_bit4,
    latency_ms_plus_status_code_high,
    flag_bit4_rank,
    flag_bit4_dense_rank_2
FROM metrics
ORDER BY flag_bit4_rank NULLS LAST;
