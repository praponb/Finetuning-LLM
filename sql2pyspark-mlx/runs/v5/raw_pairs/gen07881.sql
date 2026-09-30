WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        attr_key,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS attr_key, v
),
unpacked AS (
    -- Semi-structured parsing
    SELECT
        id,
        host,
        log_ts,
        level,
        status_code,
        attr_key,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        CAST((status_code & 2) != 0 AS INT) AS flag_bit2
    FROM lines
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT *, flag_bit2 - status_code_high AS flag_bit2_minus_status_code_high
    FROM unpacked
    WHERE attr_key IS NOT NULL
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        log_ts,
        level,
        attr_key,
        flag_bit2,
        AVG(status_code) OVER (PARTITION BY attr_key ORDER BY flag_bit2, id, attr_key ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS status_code_avg_3_prior,
        ROUND(status_code / SUM(status_code) OVER (PARTITION BY host), 4) AS status_code_share,
        ROW_NUMBER() OVER (PARTITION BY level ORDER BY status_code_high DESC, id, attr_key) AS event_seq,
        AVG(status_code_high) OVER w_ordered AS status_code_high_avg_6
    FROM filtered
    WINDOW w_ordered AS (PARTITION BY host, level ORDER BY flag_bit2, id, attr_key ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, LOWER(attr_key) AS attr_key_lower
    FROM scored
    WHERE attr_key IS NOT NULL
      AND log_ts > TO_TIMESTAMP('2025-06-01 05:00:00')
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        log_ts,
        level,
        attr_key,
        status_code_avg_3_prior,
        status_code_share,
        event_seq,
        status_code_high_avg_6,
        attr_key_lower
    FROM (
        SELECT *, RANK() OVER (PARTITION BY level, attr_key ORDER BY status_code_avg_3_prior DESC) AS row_num
        FROM base
    ) x
    WHERE row_num = 1
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY attr_key, level ORDER BY log_ts DESC, id, attr_key) AS latest_rank
        FROM deduped
    ) ranked
    WHERE latest_rank = 1
),
wide AS (
    -- Native SQL PIVOT
    SELECT *
    FROM (
        SELECT
            level,
            attr_key,
            status_code_share
        FROM ranked
    ) src
    PIVOT (
        SUM(status_code_share) AS amount,
        COUNT(1) AS cnt
        FOR attr_key IN ('region' AS region, 'trace' AS trace, 'version' AS version)
    )
)
SELECT
    level,
    region_cnt,
    trace_amount,
    trace_cnt,
    version_amount,
    version_cnt
FROM wide
ORDER BY trace_amount DESC NULLS LAST, version_cnt DESC
