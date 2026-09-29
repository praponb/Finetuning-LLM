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
    LATERAL VIEW OUTER EXPLODE(context) li AS attr_key, attr_value
),
flattened_attrs AS (
    -- Higher-order array functions
    SELECT
        host,
        latency_ms,
        status_code,
        attr_value,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        CAST((status_code & 1) != 0 AS INT) AS has_fault
    FROM lines
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT *, ROUND(status_code_high * 1.1, 2) status_code_high_scaled
    FROM flattened_attrs
    WHERE has_fault >= 1
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT *, AVG(status_code_high) OVER w_part host_avg_status_code_high
    FROM filtered
    WINDOW w_part AS (PARTITION BY host)
),
enriched AS (
    -- Ranking and gap features
    SELECT
        latency_ms,
        attr_value,
        status_code_high,
        status_code_high_scaled,
        PERCENT_RANK() OVER (PARTITION BY host ORDER BY status_code_high DESC) status_code_high_percent_rank,
        DENSE_RANK() OVER w_part has_fault_dense_rank,
        RANK() OVER (ORDER BY status_code_high_scaled ASC NULLS LAST) status_code_high_scaled_rank,
        DENSE_RANK() OVER w_part has_fault_dense_rank_2,
        DENSE_RANK() OVER (PARTITION BY host ORDER BY status_code_high_scaled ASC NULLS LAST) status_code_high_scaled_dense_rank
    FROM metrics
    WHERE status_code BETWEEN 345 AND 386
    WINDOW w_part AS (PARTITION BY host ORDER BY has_fault DESC NULLS LAST)
),
metrics_app_logs AS (
    -- Ranking and gap features
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY status_code_high_percent_rank DESC NULLS LAST) status_code_high_percent_rank_dense_rank,
        RANK() OVER (ORDER BY latency_ms DESC) latency_ms_rank,
        RANK() OVER (ORDER BY status_code_high_scaled_dense_rank ASC NULLS LAST) status_code_high_scaled_dense_rank_rank
    FROM enriched
)
SELECT
    latency_ms,
    status_code_high_scaled,
    status_code_high_percent_rank,
    has_fault_dense_rank,
    status_code_high_scaled_rank,
    has_fault_dense_rank_2,
    status_code_high_scaled_dense_rank,
    status_code_high_scaled_dense_rank_rank
FROM metrics_app_logs
ORDER BY latency_ms ASC NULLS LAST;
