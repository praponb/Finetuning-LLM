WITH latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY reading_ts DESC, id ASC) AS rank_in_group
        FROM device_readings
    ) t
    WHERE rank_in_group = 1
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC) IS NULL
                 OR reading_ts > LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC) + INTERVAL '30' MINUTE THEN 1 ELSE 0 END AS session_start_flag
    FROM latest
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM gaps
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        firmware,
        rank_in_group,
        session_start_flag,
        session_id,
        CAST(status_bits AS DOUBLE) AS status_bits_str
    FROM sessionized
    WHERE reading_ts > DATE '2025-09-01'
      AND humidity >= 43
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY humidity ASC) AS humidity_cume_dist,
        SUM(humidity) OVER (ORDER BY status_bits_str ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_humidity,
        SUM(status_bits_str) OVER (ORDER BY CAST(reading_ts AS BIGINT) ASC RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS status_bits_str_sum_2h
    FROM filtered
)
SELECT
    id,
    reading_ts,
    temp_c,
    humidity,
    pressure,
    firmware,
    rank_in_group,
    session_start_flag,
    session_id
FROM enriched;
