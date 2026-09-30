WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        key,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) x AS key, v
),
base AS (
    -- Basic cleansing and defaults
    SELECT *, LENGTH(key) AS key_length
    FROM lines
    WHERE key IN ('region', 'version', 'trace')
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(key_length) OVER w_hist AS next_key_length,
        LAG(key_length) OVER w_hist AS prev_key_length,
        ROW_NUMBER() OVER w_hist AS row_seq
    FROM base
    WINDOW w_hist AS (PARTITION BY key ORDER BY key_length, id, key)
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY key ORDER BY key_length, id, key) AS seq_no,
        LAG(next_key_length, 1) OVER (PARTITION BY key ORDER BY prev_key_length, id, key) AS prev_next_key_length
    FROM with_windows
)
SELECT
    id,
    key,
    v,
    key_length,
    next_key_length,
    prev_next_key_length,
    CASE WHEN key RLIKE '^(version|region)$' THEN 'OTHER_ZONE' WHEN key RLIKE '^(trace)$' THEN 'CORE' ELSE 'GROUP_A' END AS key_zone
FROM scored
ORDER BY key_length DESC
