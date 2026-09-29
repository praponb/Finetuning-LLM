WITH gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN reading_ts - LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) > INTERVAL '45' MINUTE THEN 1 ELSE 0 END AS new_group_flag
    FROM device_readings
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        status_bits,
        firmware,
        SUM(new_group_flag) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM gaps
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(status_bits) OVER w_grp AS status_bits_avg_30m,
        SUM(temp_c) OVER w_part AS cumulative_temp_c
    FROM sessionized
    WHERE firmware = '1.0.3'
    WINDOW w_grp AS (PARTITION BY device_code ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW), w_part AS (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        temp_c,
        status_bits,
        firmware,
        session_id,
        status_bits_avg_30m,
        COALESCE(status_bits_avg_30m, 0) AS status_bits_avg_30m_filled
    FROM with_windows
    WHERE reading_ts >= DATE '2025-09-01'
      AND firmware <> '1.0.3'
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(10) OVER (ORDER BY status_bits_avg_30m_filled DESC, id) AS status_bits_avg_30m_filled_bucket,
        SUM(session_id) OVER (ORDER BY status_bits_avg_30m, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_session_id,
        SUM(temp_c) OVER (ORDER BY session_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_temp_c,
        LAST_VALUE(status_bits_avg_30m) IGNORE NULLS OVER (ORDER BY status_bits, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_status_bits_avg_30m
    FROM base
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        GREATEST(session_id, last_known_status_bits_avg_30m) AS greatest_session_id_last_known_status_bits_avg_30m,
        LEAST(running_temp_c, session_id) AS least_running_temp_c_session_id
    FROM scored
    WHERE firmware = '1.0.3'
),
report AS (
    SELECT
        id,
        temp_c,
        firmware,
        status_bits_avg_30m_filled,
        running_session_id,
        running_temp_c,
        last_known_status_bits_avg_30m,
        least_running_temp_c_session_id
    FROM cleaned
    ORDER BY running_temp_c, firmware
)
SELECT
    id,
    temp_c,
    status_bits_avg_30m_filled,
    running_temp_c,
    least_running_temp_c_session_id
FROM report
ORDER BY status_bits_avg_30m_filled, temp_c;
