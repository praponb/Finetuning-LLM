WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        log_ts,
        status_code,
        attr_key,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS attr_key, attr_value
),
flattened_attrs AS (
    -- Higher-order array functions
    SELECT
        id,
        service,
        log_ts,
        status_code,
        attr_value,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        CAST((status_code & 2) != 0 AS INT) AS has_fault
    FROM lines
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        service,
        log_ts,
        status_code,
        attr_value,
        TRIM(service) AS clean_service,
        CASE WHEN status_code >= 467 THEN 'High' WHEN status_code >= 252 THEN 'Medium' ELSE 'Low' END AS status_code_band
    FROM flattened_attrs
    WHERE has_fault < 1
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        ROUND(status_code / SUM(status_code) OVER (PARTITION BY service), 4) AS status_code_share,
        ROUND(status_code / SUM(status_code) OVER (PARTITION BY service), 4) AS status_code_share_2,
        CUME_DIST() OVER (PARTITION BY service ORDER BY status_code DESC) AS status_code_cume_dist
    FROM filtered
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        status_code,
        status_code_band,
        status_code_share,
        status_code_share_2,
        PERCENT_RANK() OVER w_hist AS status_code_share_2_percent_rank
    FROM enriched
    WHERE status_code_share_2 IS NOT NULL
    WINDOW w_hist AS (PARTITION BY service ORDER BY status_code_share_2 DESC)
)
SELECT
    id,
    status_code,
    status_code_band,
    status_code_share,
    status_code_share_2_percent_rank
FROM scored
