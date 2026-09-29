WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        log_ts,
        level,
        latency_ms,
        status_code,
        k,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) x AS k, value
),
unpacked AS (
    -- Semi-structured parsing
    SELECT
        id,
        service,
        k,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        CAST((status_code & 2) != 0 AS INT) AS has_fault
    FROM exploded
    WHERE k = 'trace'
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        service,
        k
    FROM unpacked
    QUALIFY ROW_NUMBER() OVER (PARTITION BY service ORDER BY status_code_high NULLS FIRST, id, k) = 1
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, INITCAP(TRIM(k)) AS k_title
    FROM latest
    WHERE service = 'checkout'
),
valid AS (
    -- Keep recent, valid rows
    SELECT *, INITCAP(TRIM(k)) AS k_title_2
    FROM cleaned
    WHERE k IS NOT NULL
),
cleaned_app_logs AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        service,
        k,
        k_title,
        k_title_2,
        CONCAT_WS('-', service, k) AS service_k_key,
        COALESCE(k, 'UNKNOWN') AS k_filled
    FROM valid
    WHERE k = 'region'
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CASE WHEN service = 'checkout' THEN 1 ELSE 0 END AS is_checkout
    FROM cleaned_app_logs
    WHERE service = 'billing'
)
-- Window metrics over several different partitions
SELECT
    *,
    NTILE(10) OVER (ORDER BY is_checkout DESC, id, k) AS is_checkout_bucket,
    LEAD(is_checkout, 1) OVER (ORDER BY is_checkout, id, k) AS next_is_checkout,
    FIRST_VALUE(is_checkout) IGNORE NULLS OVER (PARTITION BY service, k ORDER BY is_checkout, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_is_checkout,
    FIRST_VALUE(k, TRUE) OVER (PARTITION BY service ORDER BY is_checkout, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_k,
    FIRST_VALUE(k) IGNORE NULLS OVER (PARTITION BY k ORDER BY is_checkout, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_k_2
FROM filtered
WHERE service != 'checkout';
