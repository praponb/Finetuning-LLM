WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        status_code,
        k,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS k, attr_value
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY host ORDER BY status_code DESC, id, k) AS row_num
    FROM lines
    QUALIFY row_num = 1
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        host,
        status_code,
        k,
        attr_value,
        CASE WHEN status_code >= 442 THEN 'High' WHEN status_code >= 264 THEN 'Medium' ELSE 'Low' END AS status_code_band
    FROM latest
    WHERE k IS DISTINCT FROM 'trace'
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        COUNT(status_code) OVER (PARTITION BY k ORDER BY status_code, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_status_code,
        MIN(status_code) OVER (PARTITION BY k ORDER BY status_code, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_status_code,
        MAX(status_code) OVER (PARTITION BY k ORDER BY status_code, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_status_code
    FROM filtered
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        FLOOR(running_max_status_code) AS running_max_status_code_whole,
        COALESCE(k, 'UNKNOWN') AS k_filled
    FROM enriched
    WHERE running_count_status_code <= 7
),
filtered_app_logs AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        k,
        attr_value,
        running_count_status_code,
        running_min_status_code,
        running_max_status_code_whole,
        k_filled,
        TRIM(host) AS clean_host
    FROM cleaned
    WHERE running_min_status_code > 390
)
SELECT
    id,
    k,
    attr_value,
    running_min_status_code,
    running_max_status_code_whole,
    k_filled
FROM filtered_app_logs;
