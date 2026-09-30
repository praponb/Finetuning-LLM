WITH enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LEAD(pressure) OVER w_recent AS next_pressure,
        LEAD(humidity) OVER w_recent AS next_humidity,
        ROW_NUMBER() OVER w_recent AS row_seq,
        AVG(pressure) OVER (PARTITION BY device_code ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS pressure_avg_15m
    FROM device_readings
    WINDOW w_recent AS (PARTITION BY device_code ORDER BY reading_ts, id)
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        device_code,
        temp_c,
        humidity,
        pressure,
        next_pressure,
        next_humidity,
        pressure_avg_15m,
        LEAD(next_humidity, 2) OVER (PARTITION BY device_code ORDER BY next_humidity, id) AS next_next_humidity,
        PERCENT_RANK() OVER (PARTITION BY device_code ORDER BY temp_c) AS temp_c_percent_rank,
        LEAD(next_humidity, 1) OVER (PARTITION BY device_code ORDER BY next_pressure, id) AS next_next_humidity_2
    FROM enriched
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        device_code,
        humidity,
        next_pressure,
        pressure_avg_15m,
        next_next_humidity,
        next_next_humidity_2,
        AVG(pressure) OVER (PARTITION BY device_code ORDER BY next_next_humidity, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS pressure_avg_3_prior,
        LAG(pressure, 1) OVER (PARTITION BY device_code ORDER BY temp_c, id) AS prev_pressure,
        LAST_VALUE(pressure, TRUE) OVER (ORDER BY next_next_humidity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_pressure,
        CORR(temp_c, pressure) OVER (PARTITION BY device_code) AS corr_temp_c_pressure
    FROM scored
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN pressure_avg_15m >= 1023 THEN 'High' WHEN pressure_avg_15m >= 998 THEN 'Medium' ELSE 'Low' END AS pressure_avg_15m_level,
        humidity + pressure_avg_3_prior AS humidity_plus_pressure_avg_3_prior
    FROM with_windows
    WHERE next_next_humidity_2 >= 37
),
enriched_device_readings AS (
    -- Ranking and gap features
    SELECT
        id,
        humidity,
        next_pressure,
        next_next_humidity_2,
        pressure_avg_3_prior,
        pressure_avg_15m_level,
        ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY next_next_humidity DESC, id) AS event_seq,
        AVG(next_next_humidity_2) OVER (PARTITION BY device_code ORDER BY humidity_plus_pressure_avg_3_prior, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS next_next_humidity_2_avg_5_prior,
        LAG(humidity_plus_pressure_avg_3_prior, 1) OVER (ORDER BY corr_temp_c_pressure, id) AS prev_humidity_plus_pressure_avg_3_prior,
        ROW_NUMBER() OVER (ORDER BY humidity_plus_pressure_avg_3_prior, id) AS seq_no
    FROM cleaned
    WHERE pressure_avg_15m >= 1020
)
SELECT
    id,
    humidity,
    next_pressure,
    next_next_humidity_2,
    pressure_avg_15m_level,
    event_seq,
    next_next_humidity_2_avg_5_prior,
    prev_humidity_plus_pressure_avg_3_prior,
    seq_no
FROM enriched_device_readings
ORDER BY humidity DESC NULLS LAST, event_seq
