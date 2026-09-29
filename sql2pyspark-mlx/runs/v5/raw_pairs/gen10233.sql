WITH filtered AS (
    SELECT
        id,
        device_code,
        reading_ts,
        humidity,
        pressure,
        firmware,
        ROUND(humidity, 0) humidity_rounded
    FROM device_readings
    WHERE reading_ts > TO_TIMESTAMP('2025-09-01 06:00:00')
),
ranked AS (
    SELECT
        id,
        reading_ts,
        firmware
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY humidity DESC, id) AS row_num
        FROM filtered
    ) ranked
    WHERE row_num = 1
),
cleaned AS (
    SELECT *, CAST(reading_ts AS DATE) reading_ts_day
    FROM ranked
    WHERE reading_ts >= '2023-08-01'
),
with_windows AS (
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY reading_ts, id) seq_no,
        ROW_NUMBER() OVER (ORDER BY reading_ts, id) row_seq,
        ROW_NUMBER() OVER (ORDER BY reading_ts_day DESC, id) row_seq_2
    FROM cleaned
),
with_windows_device_readings AS (
    SELECT
        *,
        MAX(row_seq_2) OVER w_recent running_max_row_seq_2,
        AVG(row_seq_2) OVER w_recent running_avg_row_seq_2,
        MIN(row_seq_2) OVER w_recent running_min_row_seq_2
    FROM with_windows
    WINDOW w_recent AS (ORDER BY seq_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
cleaned_device_readings AS (
    SELECT
        id,
        firmware,
        reading_ts_day,
        seq_no,
        row_seq,
        row_seq_2,
        running_min_row_seq_2,
        running_max_row_seq_2 * seq_no running_max_row_seq_2_times_seq_no
    FROM with_windows_device_readings
    WHERE reading_ts > TO_TIMESTAMP('2025-09-01 04:00:00')
      AND YEAR(reading_ts_day) = 2025
)
SELECT
    id,
    firmware,
    reading_ts_day,
    row_seq_2,
    running_min_row_seq_2
FROM cleaned_device_readings
ORDER BY reading_ts_day, running_min_row_seq_2 DESC;
