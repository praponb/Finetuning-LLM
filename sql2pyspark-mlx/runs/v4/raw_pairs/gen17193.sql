WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        FLOOR(humidity) AS humidity_whole,
        GREATEST(temp_c, status_bits) AS greatest_temp_c_status_bits
    FROM device_readings
    WHERE status_bits < 8
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        humidity_whole,
        greatest_temp_c_status_bits,
        SUM(status_bits) OVER (ORDER BY humidity_whole ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_status_bits,
        ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY pressure DESC, id ASC) AS row_seq,
        AVG(status_bits) OVER (ORDER BY status_bits ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS status_bits_avg_6,
        NTILE(4) OVER (PARTITION BY device_code ORDER BY humidity_whole ASC, id ASC) AS humidity_whole_bucket
    FROM cleaned
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        humidity_whole,
        greatest_temp_c_status_bits,
        running_status_bits,
        row_seq,
        status_bits_avg_6,
        humidity_whole_bucket,
        ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY status_bits_avg_6 ASC, id ASC) AS event_seq,
        ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY running_status_bits DESC, id ASC) AS seq_no,
        AVG(humidity) OVER (PARTITION BY device_code ORDER BY greatest_temp_c_status_bits ASC, id ASC ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS humidity_avg_6_prior
    FROM with_windows
),
filtered AS (
    -- Keep recent, valid rows
    SELECT *, LEAST(event_seq, humidity_whole_bucket) AS least_event_seq_humidity_whole_bucket
    FROM metrics
    WHERE temp_c BETWEEN 31 AND 35
),
with_windows_device_readings AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(event_seq, 1) OVER (PARTITION BY device_code ORDER BY status_bits ASC, id ASC) AS next_event_seq,
        SUM(status_bits) OVER (PARTITION BY device_code ORDER BY CAST(reading_ts AS BIGINT) ASC RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS status_bits_sum_30m,
        PERCENT_RANK() OVER w_grp AS humidity_percent_rank
    FROM filtered
    WINDOW w_grp AS (PARTITION BY device_code ORDER BY humidity DESC NULLS LAST)
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY least_event_seq_humidity_whole_bucket ASC NULLS LAST, id ASC) AS row_num
    FROM with_windows_device_readings
    QUALIFY row_num = 1
),
daily AS (
    -- Grouped metrics
    SELECT
        DATE_TRUNC('HOUR', reading_ts) AS hour_window,
        COUNT(*) AS num_rows,
        BIT_OR(status_bits) AS status_bits_bit_or,
        COUNT(CASE WHEN device_code = 'DV-04' THEN 1 END) AS dv_04_count,
        BIT_AND(status_bits) AS status_bits_bit_and
    FROM deduped
    GROUP BY DATE_TRUNC('HOUR', reading_ts)
)
SELECT hour_window, dv_04_count
FROM daily
ORDER BY dv_04_count DESC
