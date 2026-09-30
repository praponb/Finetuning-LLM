WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        status_code,
        attr_key,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS attr_key, v
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, LOWER(attr_key) AS attr_key_lower
    FROM lines
    WHERE attr_key IS NOT NULL
      AND attr_key IS NOT DISTINCT FROM 'region'
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        service,
        v,
        ROUND(status_code / SUM(status_code) OVER (PARTITION BY attr_key), 4) AS status_code_share,
        PERCENT_RANK() OVER w_running AS status_code_percent_rank,
        ROUND(status_code / SUM(status_code) OVER (PARTITION BY attr_key, service), 4) AS status_code_share_2,
        ROUND(status_code / SUM(status_code) OVER (PARTITION BY attr_key), 4) AS status_code_share_3,
        PERCENT_RANK() OVER w_running AS status_code_percent_rank_2
    FROM base
    WINDOW w_running AS (ORDER BY status_code DESC)
),
scored AS (
    -- Window metrics over several different partitions
    SELECT *, CUME_DIST() OVER w_recent AS status_code_percent_rank_cume_dist
    FROM metrics
    WINDOW w_recent AS (PARTITION BY service ORDER BY status_code_percent_rank DESC NULLS FIRST)
)
SELECT
    service,
    v,
    status_code_share,
    status_code_percent_rank,
    status_code_percent_rank_2,
    status_code_percent_rank_cume_dist,
    CASE WHEN service RLIKE '^(auth|billing)$' THEN 'EXTENDED' WHEN service RLIKE '^(search|checkout)$' THEN 'CORE' ELSE 'GROUP_A' END AS service_zone
FROM scored
