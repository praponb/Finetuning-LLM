WITH exploded AS (
    SELECT
        id,
        service,
        log_ts,
        level,
        status_code,
        k,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) t AS k, value
),
parsed AS (
    SELECT
        id,
        log_ts,
        k,
        value,
        CAST((status_code & 2) != 0 AS INT) AS is_flagged,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM exploded
),
gaps AS (
    SELECT *, CASE WHEN LAG(log_ts) OVER (PARTITION BY k ORDER BY log_ts, id, k) IS NULL
                 OR log_ts > LAG(log_ts) OVER (PARTITION BY k ORDER BY log_ts, id, k) + INTERVAL '20' MINUTE THEN 1 ELSE 0 END AS is_new_session
    FROM parsed
),
islands AS (
    SELECT
        id,
        log_ts,
        k,
        SUM(is_new_session) OVER (PARTITION BY k ORDER BY log_ts, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM gaps
),
with_windows AS (
    SELECT
        id,
        k,
        island_id,
        LEAD(island_id, 2, 0) OVER (PARTITION BY k ORDER BY log_ts, id, k) AS next_island_id,
        SUM(island_id) OVER w_grp AS island_id_sum_1h
    FROM islands
    WHERE island_id BETWEEN 5 AND 7
    WINDOW w_grp AS (PARTITION BY k ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    k,
    ROW_NUMBER() OVER (PARTITION BY k ORDER BY island_id_sum_1h, id, k) AS row_seq,
    MIN(island_id_sum_1h) OVER w_running AS k_min_island_id_sum_1h,
    NTILE(10) OVER w_ordered AS island_id_bucket,
    LEAD(island_id_sum_1h, 1) OVER (PARTITION BY k ORDER BY next_island_id, id, k) AS next_island_id_sum_1h
FROM with_windows
WINDOW w_running AS (PARTITION BY k), w_ordered AS (ORDER BY island_id, id, k);
