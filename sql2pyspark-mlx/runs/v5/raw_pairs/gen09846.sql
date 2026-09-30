WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        status_code,
        attr_key,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) li AS attr_key, value
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        service,
        host,
        status_code,
        attr_key,
        value,
        CASE WHEN status_code >= 424 THEN 'High' WHEN status_code >= 242 THEN 'Medium' ELSE 'Low' END AS status_code_tier
    FROM exploded
    WHERE attr_key = 'version'
),
metrics AS (
    -- Ranking and gap features
    SELECT *, MIN(status_code) OVER (PARTITION BY service) AS service_min_status_code
    FROM filtered
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(status_code / SUM(status_code) OVER (PARTITION BY attr_key), 4) AS status_code_share,
        MAX(status_code) OVER w_grp AS attr_key_max_status_code
    FROM metrics
    WINDOW w_grp AS (PARTITION BY attr_key, host)
),
with_windows_app_logs AS (
    -- Ranking and gap features
    SELECT
        *,
        DENSE_RANK() OVER w_grp AS service_min_status_code_dense_rank,
        RANK() OVER (PARTITION BY service ORDER BY service_min_status_code DESC) AS service_min_status_code_rank,
        STDDEV_SAMP(status_code) OVER (PARTITION BY host, attr_key) AS host_stddev_samp_status_code,
        DENSE_RANK() OVER (PARTITION BY attr_key ORDER BY attr_key_max_status_code DESC NULLS LAST) AS attr_key_max_status_code_dense_rank
    FROM with_windows
    WINDOW w_grp AS (PARTITION BY host ORDER BY service_min_status_code DESC NULLS LAST)
)
SELECT
    id,
    service,
    host,
    status_code,
    attr_key,
    value,
    status_code_tier,
    service_min_status_code,
    attr_key_max_status_code,
    CASE WHEN host RLIKE '^(h3|h1)$' THEN 'GROUP_B' WHEN host RLIKE '^(h2|h4)$' THEN 'EXTENDED' ELSE 'OTHER_ZONE' END AS host_zone
FROM with_windows_app_logs
