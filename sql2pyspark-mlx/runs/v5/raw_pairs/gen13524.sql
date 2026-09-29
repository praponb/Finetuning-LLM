WITH flattened AS (
    SELECT
        id,
        service,
        host,
        status_code,
        key,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS key, attr_value
),
parsed AS (
    SELECT
        id,
        status_code,
        key,
        CAST((status_code & 4) != 0 AS INT) AS flag_bit4,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM flattened
),
cleaned AS (
    SELECT
        id,
        key,
        flag_bit4,
        status_code_high,
        COALESCE(key, 'Unknown') AS key_filled,
        COALESCE(key, 'N/A') AS key_clean
    FROM parsed
    WHERE key IS DISTINCT FROM 'region'
),
base AS (
    SELECT
        *,
        CAST(flag_bit4 AS STRING) AS flag_bit4_str,
        COALESCE(key, 'OTHER') AS key_clean_2
    FROM cleaned
    WHERE key = 'trace'
),
enriched AS (
    SELECT *, PERCENT_RANK() OVER (PARTITION BY key ORDER BY status_code_high DESC) AS status_code_high_percent_rank
    FROM base
    WHERE flag_bit4 >= 1
)
SELECT
    flag_bit4_str,
    key_clean_2,
    status_code_high_percent_rank
FROM enriched
ORDER BY status_code_high_percent_rank;
