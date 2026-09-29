WITH lines AS (
    SELECT
        id,
        service,
        log_ts,
        level,
        latency_ms,
        status_code,
        attr_key,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS attr_key, attr_value
),
flattened_attrs AS (
    SELECT
        id,
        service,
        attr_key,
        CAST((status_code & 8) != 0 AS INT) AS flag_bit8,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM lines
),
with_windows AS (
    SELECT
        *,
        SUM(flag_bit8) OVER w_hist AS running_flag_bit8,
        NTILE(3) OVER w_recent AS status_code_high_bucket
    FROM flattened_attrs
    WINDOW w_hist AS (PARTITION BY service ORDER BY status_code_high, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_recent AS (PARTITION BY service ORDER BY status_code_high, id, attr_key)
),
enriched AS (
    SELECT
        id,
        attr_key,
        RANK() OVER (PARTITION BY service ORDER BY status_code_high DESC NULLS LAST) AS status_code_high_rank,
        COUNT(running_flag_bit8) OVER (PARTITION BY service) AS service_count_running_flag_bit8
    FROM with_windows
    WHERE status_code_high_bucket <= 5
),
latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY attr_key ORDER BY status_code_high_rank DESC NULLS FIRST, id, attr_key) AS rn
    FROM enriched
    QUALIFY rn = 1
),
metrics AS (
    SELECT
        *,
        COUNT(service_count_running_flag_bit8) OVER w_hist AS running_count_service_count_running_flag_bit8,
        AVG(service_count_running_flag_bit8) OVER w_hist AS running_avg_service_count_running_flag_bit8,
        MAX(service_count_running_flag_bit8) OVER w_hist AS running_max_service_count_running_flag_bit8,
        SUM(service_count_running_flag_bit8) OVER w_hist AS cumulative_service_count_running_flag_bit8
    FROM latest
    WINDOW w_hist AS (PARTITION BY attr_key ORDER BY status_code_high_rank, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
base AS (
    SELECT
        *,
        status_code_high_rank * running_count_service_count_running_flag_bit8 AS status_code_high_rank_times_running_count_service_count_running_flag_bit8,
        ROUND(rn / running_avg_service_count_running_flag_bit8, 2) AS rn_per_running_avg_service_count_running_flag_bit8,
        COALESCE(attr_key, 'NONE') AS attr_key_clean
    FROM metrics
    WHERE attr_key IS DISTINCT FROM 'version'
)
SELECT
    id,
    attr_key,
    service_count_running_flag_bit8,
    rn,
    running_avg_service_count_running_flag_bit8,
    running_max_service_count_running_flag_bit8,
    cumulative_service_count_running_flag_bit8,
    status_code_high_rank_times_running_count_service_count_running_flag_bit8,
    rn_per_running_avg_service_count_running_flag_bit8,
    CASE WHEN attr_key RLIKE '^(trace|region)$' THEN 'GROUP_A' ELSE 'PRIORITY' END AS attr_key_zone
FROM base
ORDER BY rn_per_running_avg_service_count_running_flag_bit8 DESC NULLS LAST, id
