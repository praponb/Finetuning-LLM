WITH lines AS (
    SELECT
        id,
        host,
        level,
        status_code,
        key,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS key, attr_value
),
deduped AS (
    SELECT *
    FROM lines
    QUALIFY RANK() OVER (PARTITION BY host, key ORDER BY status_code) = 1
),
deduped_app_logs AS (
    SELECT *, RANK() OVER (PARTITION BY level, host ORDER BY status_code DESC) AS latest_rank
    FROM deduped
    QUALIFY latest_rank = 1
)
SELECT
    id,
    status_code,
    key,
    attr_value,
    latest_rank,
    CASE WHEN key RLIKE '^(trace|region)$' THEN 'EXTENDED' ELSE 'GROUP_A' END AS key_zone
FROM deduped_app_logs
ORDER BY key_zone DESC NULLS FIRST
