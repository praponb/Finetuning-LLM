WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, LENGTH(device_code) AS device_code_length
    FROM device_readings
    WHERE reading_ts > DATE '2025-09-01'
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        humidity,
        pressure,
        status_bits,
        AVG(temp_c) OVER (ORDER BY humidity, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS temp_c_avg_6,
        LAG(humidity, 1, 0) OVER (ORDER BY humidity, id) AS prev_humidity,
        AVG(status_bits) OVER (PARTITION BY device_code ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS status_bits_avg_1h,
        PERCENT_RANK() OVER (PARTITION BY device_code ORDER BY temp_c DESC) AS temp_c_percent_rank
    FROM filtered
),
with_windows_device_readings AS (
    -- Ranking and gap features
    SELECT
        id,
        humidity,
        prev_humidity,
        AVG(status_bits) OVER w_part AS running_avg_status_bits,
        COUNT(status_bits) OVER w_part AS running_count_status_bits,
        SUM(status_bits) OVER w_part AS running_sum_status_bits
    FROM with_windows
    WINDOW w_part AS (ORDER BY temp_c_percent_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        humidity,
        prev_humidity,
        ROW_NUMBER() OVER (ORDER BY running_avg_status_bits DESC, id) AS row_seq,
        FIRST_VALUE(running_avg_status_bits) IGNORE NULLS OVER (ORDER BY running_count_status_bits, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_running_avg_status_bits,
        ROW_NUMBER() OVER (ORDER BY running_count_status_bits DESC, id) AS event_seq,
        AVG(running_sum_status_bits) OVER (ORDER BY prev_humidity, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS running_sum_status_bits_avg_3
    FROM with_windows_device_readings
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        humidity,
        prev_humidity,
        first_known_running_avg_status_bits,
        event_seq,
        ROUND(row_seq / event_seq, 2) AS row_seq_per_event_seq
    FROM scored
    WHERE row_seq < 7
),
report AS (
    SELECT
        id,
        humidity,
        prev_humidity,
        first_known_running_avg_status_bits,
        row_seq_per_event_seq
    FROM cleaned
    ORDER BY row_seq_per_event_seq DESC, prev_humidity DESC
)
SELECT
    id,
    prev_humidity,
    first_known_running_avg_status_bits,
    row_seq_per_event_seq
FROM report
ORDER BY row_seq_per_event_seq DESC NULLS FIRST, prev_humidity;
