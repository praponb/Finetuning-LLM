WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        key,
        value
    FROM carts
    LATERAL VIEW EXPLODE(attributes) x AS key, value
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(created_at) OVER (PARTITION BY key ORDER BY created_at ASC, id ASC, key ASC) IS NULL
                 OR created_at > LAG(created_at) OVER (PARTITION BY key ORDER BY created_at ASC, id ASC, key ASC) + INTERVAL 1 HOUR THEN 1 ELSE 0 END AS session_start_flag
    FROM lines
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY key ORDER BY created_at ASC, id ASC, key ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        created_at,
        key,
        value,
        session_start_flag,
        burst_id,
        UPPER(key) AS key_upper
    FROM islands
    WHERE created_at > TO_DATE('2025-03-02')
      AND key IS NOT NULL
      AND created_at < '2025-01-06'
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY key ORDER BY session_start_flag ASC NULLS FIRST, id ASC, key ASC) AS rn
    FROM valid
    QUALIFY rn = 1
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        created_at,
        key,
        value,
        session_start_flag,
        burst_id,
        key_upper,
        rn,
        LAG(burst_id, 1) OVER (PARTITION BY key ORDER BY session_start_flag ASC, id ASC, key ASC) AS prev_burst_id,
        PERCENT_RANK() OVER (PARTITION BY key ORDER BY burst_id DESC) AS burst_id_percent_rank,
        ROW_NUMBER() OVER (PARTITION BY key ORDER BY session_start_flag ASC, id ASC, key ASC) AS event_seq,
        PERCENT_RANK() OVER (ORDER BY burst_id DESC) AS burst_id_percent_rank_2,
        LAST_VALUE(session_start_flag, TRUE) OVER (ORDER BY session_start_flag ASC, id ASC, key ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_session_start_flag
    FROM deduped
)
SELECT
    id,
    key,
    value,
    session_start_flag,
    burst_id,
    key_upper,
    rn,
    prev_burst_id,
    burst_id_percent_rank,
    CASE WHEN key RLIKE '^(device|utm_source)$' THEN 'GROUP_A' ELSE 'EXTENDED' END AS key_zone
FROM metrics
ORDER BY burst_id_percent_rank ASC NULLS LAST;
