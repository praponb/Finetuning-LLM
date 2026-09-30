WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        log_ts,
        level,
        status_code,
        attr_key,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) x AS attr_key, v
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT *, CONCAT_WS('/', level, service) AS level_service_key
    FROM lines
    WHERE attr_key IS NOT NULL
      AND attr_key IS DISTINCT FROM 'region'
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        RANK() OVER w_running AS status_code_rank,
        SUM(status_code) OVER (PARTITION BY service ORDER BY status_code, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_status_code
    FROM filtered
    WINDOW w_running AS (PARTITION BY attr_key ORDER BY status_code DESC)
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM with_windows
    QUALIFY ROW_NUMBER() OVER (PARTITION BY service ORDER BY status_code_rank DESC, id, attr_key) = 1
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY attr_key ORDER BY log_ts DESC, id, attr_key) AS event_seq,
        AVG(status_code_rank) OVER (PARTITION BY service ORDER BY status_code_rank, id, attr_key ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS status_code_rank_avg_6_prior,
        ROW_NUMBER() OVER (PARTITION BY service ORDER BY cumulative_status_code, id, attr_key) AS row_seq,
        RANK() OVER (PARTITION BY attr_key ORDER BY status_code) AS status_code_rank_2
    FROM ranked
)
SELECT
    id,
    service,
    log_ts,
    attr_key,
    v,
    level_service_key,
    status_code_rank,
    cumulative_status_code,
    row_seq
FROM metrics
ORDER BY status_code_rank NULLS FIRST;
