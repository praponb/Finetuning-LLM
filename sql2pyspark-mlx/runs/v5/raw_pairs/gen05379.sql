WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        log_ts,
        latency_ms,
        status_code,
        key,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS key, value
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        CORR(latency_ms, status_code) OVER (PARTITION BY service) corr_latency_ms_status_code,
        CUME_DIST() OVER (PARTITION BY key ORDER BY status_code DESC) status_code_cume_dist,
        LAG(latency_ms, 1, 0) OVER (PARTITION BY key, host ORDER BY status_code, id, key) prev_latency_ms,
        ROUND(status_code / SUM(status_code) OVER (PARTITION BY host), 4) status_code_share,
        NTILE(10) OVER (ORDER BY latency_ms DESC, id, key) latency_ms_bucket
    FROM exploded
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        latency_ms,
        status_code,
        key,
        corr_latency_ms_status_code,
        status_code_cume_dist,
        prev_latency_ms,
        latency_ms_bucket,
        RANK() OVER (PARTITION BY key, host ORDER BY status_code_cume_dist DESC NULLS FIRST) status_code_cume_dist_rank,
        ROW_NUMBER() OVER (PARTITION BY service, host ORDER BY log_ts DESC, id, key) seq_no,
        SUM(status_code_share) OVER w_grp cumulative_status_code_share,
        ROW_NUMBER() OVER (PARTITION BY key, service ORDER BY status_code_cume_dist DESC, id, key) event_seq
    FROM metrics
    WINDOW w_grp AS (PARTITION BY host ORDER BY prev_latency_ms, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM scored
    QUALIFY ROW_NUMBER() OVER (PARTITION BY key ORDER BY status_code_cume_dist DESC, id, key) = 1
),
report AS (
    SELECT
        id,
        latency_ms,
        status_code,
        key,
        status_code_cume_dist,
        latency_ms_bucket,
        status_code_cume_dist_rank,
        seq_no,
        cumulative_status_code_share,
        CASE WHEN key RLIKE '^(region|version)$' THEN 'CORE' ELSE 'GROUP_B' END AS key_zone
    FROM deduped
    ORDER BY status_code_cume_dist_rank DESC NULLS FIRST
)
SELECT
    id,
    latency_ms,
    status_code_cume_dist,
    latency_ms_bucket,
    status_code_cume_dist_rank,
    seq_no,
    cumulative_status_code_share,
    key_zone,
    CASE WHEN key_zone RLIKE '^(GROUP_B|CORE)$' THEN 'OTHER_ZONE' WHEN key_zone RLIKE '^(EXTENDED)$' THEN 'CORE' ELSE 'GROUP_A' END AS key_zone_zone
FROM report
ORDER BY latency_ms_bucket DESC NULLS LAST, status_code_cume_dist DESC;
