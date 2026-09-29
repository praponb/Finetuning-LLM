WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        latency_ms,
        status_code,
        k,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS k, attr_value
),
flattened_attrs AS (
    -- Struct fields, map lookups and JSON extraction
    SELECT
        id,
        service,
        host,
        status_code,
        k,
        attr_value,
        CAST((status_code & 2) != 0 AS INT) AS flag_bit2,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM exploded
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(status_code_high) OVER (PARTITION BY k ORDER BY status_code_high, id, k ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS status_code_high_avg_6_prior,
        COUNT(flag_bit2) OVER (PARTITION BY service) AS service_count_flag_bit2
    FROM flattened_attrs
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        host,
        k,
        attr_value,
        flag_bit2,
        service_count_flag_bit2
    FROM metrics
    QUALIFY RANK() OVER (PARTITION BY service, host ORDER BY status_code_high_avg_6_prior DESC) = 1
)
-- Multi-level aggregation
SELECT
    k,
    COUNT_IF(host = 'h2') AS h2_events,
    SORT_ARRAY(COLLECT_SET(k)) AS k_list,
    MAX(flag_bit2) FILTER (WHERE k = 'trace') AS trace_max_flag_bit2,
    SUM(service_count_flag_bit2) FILTER (WHERE k = 'region') AS region_total_service_count_flag_bit2
FROM deduped
GROUP BY k
ORDER BY region_total_service_count_flag_bit2 NULLS LAST;
