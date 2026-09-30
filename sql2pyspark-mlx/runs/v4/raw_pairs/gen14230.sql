WITH lines AS (
    SELECT
        id,
        service,
        log_ts,
        level,
        latency_ms,
        status_code,
        k,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS k, attr_value
),
unpacked AS (
    SELECT
        id,
        service,
        log_ts,
        status_code,
        k,
        attr_value,
        CAST((status_code & 8) != 0 AS INT) AS has_fault,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM lines
    WHERE log_ts >= '2024-05-01'
),
valid AS (
    SELECT
        id,
        service,
        log_ts,
        status_code,
        k,
        attr_value,
        status_code_high,
        UPPER(service) service_upper,
        COALESCE(k, 'OTHER') k_filled
    FROM unpacked
    WHERE k IS NOT NULL
      AND k != 'trace'
),
daily AS (
    SELECT
        CASE WHEN status_code >= 439 THEN 'LARGE' ELSE 'SMALL' END AS status_code_tier,
        COUNT(DISTINCT k) AS distinct_ks,
        MAX(status_code) AS max_status_code,
        PERCENTILE_APPROX(status_code, 0.5) AS status_code_p50,
        BIT_OR(status_code) AS status_code_bit_or,
        (UNIX_TIMESTAMP(MAX(log_ts)) - UNIX_TIMESTAMP(MIN(log_ts))) / 60 AS span_minutes,
        ROUND(SUM(status_code_high) / NULLIF(COUNT(CASE WHEN service = 'billing' THEN 1 END), 0), 2) AS status_code_high_per_billing
    FROM valid
    GROUP BY 1
)
SELECT
    status_code_tier,
    distinct_ks,
    status_code_p50,
    status_code_bit_or,
    span_minutes,
    status_code_high_per_billing
FROM daily;
