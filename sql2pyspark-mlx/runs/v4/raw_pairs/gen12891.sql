WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        level,
        attr_key,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS attr_key, attr_value
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        service,
        host,
        level,
        attr_key,
        INITCAP(TRIM(service)) service_title,
        COALESCE(attr_key, 'N/A') attr_key_filled
    FROM exploded
    WHERE attr_key IS NOT NULL
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        service,
        level,
        attr_key,
        UPPER(service) service_upper,
        CASE WHEN service = 'auth' THEN 1 ELSE 0 END is_auth,
        COALESCE(attr_key, 'OTHER') attr_key_filled_2
    FROM filtered
    WHERE attr_key = 'region'
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(is_auth, 1) OVER (PARTITION BY level ORDER BY is_auth, id, attr_key) prev_is_auth,
        LEAD(is_auth, 1) OVER w_running next_is_auth,
        SUM(is_auth) OVER (PARTITION BY service ORDER BY is_auth, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_is_auth
    FROM cleaned
    WINDOW w_running AS (PARTITION BY attr_key, service ORDER BY is_auth, id, attr_key)
),
grouped AS (
    -- Grouped metrics
    SELECT
        attr_key,
        PERCENTILE_APPROX(next_is_auth, ARRAY(0.25, 0.5, 0.75)) AS next_is_auth_quartiles,
        MAX(prev_is_auth) FILTER (WHERE service = 'auth') AS auth_max_prev_is_auth,
        COUNT(*) AS row_count
    FROM scored
    GROUP BY attr_key
)
SELECT
    attr_key,
    auth_max_prev_is_auth,
    row_count
FROM grouped
ORDER BY auth_max_prev_is_auth DESC;
