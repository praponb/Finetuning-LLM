WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        latency_ms,
        status_code,
        attr_key,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS attr_key, value
),
valid AS (
    -- Basic cleansing and defaults
    SELECT *, CASE WHEN attr_key = 'version' THEN 1 ELSE 0 END is_version
    FROM flattened
    WHERE latency_ms < 1661
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        host,
        latency_ms,
        status_code,
        attr_key,
        value,
        is_version,
        ROUND(status_code / SUM(status_code) OVER (PARTITION BY attr_key), 4) status_code_share,
        CORR(status_code, latency_ms) OVER (PARTITION BY host, attr_key) corr_status_code_latency_ms,
        COUNT(is_version) OVER (PARTITION BY attr_key, host) attr_key_count_is_version,
        MIN(latency_ms) OVER (PARTITION BY host) host_min_latency_ms
    FROM valid
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        host,
        latency_ms,
        status_code,
        attr_key,
        value,
        is_version,
        status_code_share,
        corr_status_code_latency_ms,
        attr_key_count_is_version,
        host_min_latency_ms,
        PERCENT_RANK() OVER (ORDER BY latency_ms NULLS LAST) latency_ms_percent_rank,
        RANK() OVER w_grp is_version_rank,
        RANK() OVER (ORDER BY attr_key_count_is_version NULLS LAST) attr_key_count_is_version_rank,
        PERCENT_RANK() OVER (PARTITION BY attr_key ORDER BY attr_key_count_is_version DESC NULLS LAST) attr_key_count_is_version_percent_rank,
        MAX(is_version) OVER w_running attr_key_max_is_version
    FROM with_windows
    WINDOW w_grp AS (PARTITION BY attr_key, host ORDER BY is_version DESC NULLS FIRST), w_running AS (PARTITION BY attr_key)
)
SELECT
    id,
    status_code,
    attr_key,
    value,
    is_version,
    corr_status_code_latency_ms,
    attr_key_count_is_version,
    host_min_latency_ms,
    is_version_rank
FROM enriched;
