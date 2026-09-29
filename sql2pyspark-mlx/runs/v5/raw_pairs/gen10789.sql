WITH scored AS (
    SELECT
        id,
        device_code,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        COUNT(status_bits) OVER w_running AS running_count_status_bits,
        MIN(status_bits) OVER w_running AS running_min_status_bits,
        MAX(status_bits) OVER w_running AS running_max_status_bits
    FROM device_readings
    WINDOW w_running AS (PARTITION BY device_code ORDER BY temp_c, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
metrics AS (
    SELECT
        id,
        humidity,
        pressure,
        status_bits,
        running_count_status_bits,
        AVG(temp_c) OVER (PARTITION BY device_code ORDER BY temp_c, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS temp_c_avg_3,
        ROW_NUMBER() OVER (ORDER BY running_max_status_bits, id) AS event_seq,
        PERCENT_RANK() OVER (PARTITION BY device_code ORDER BY humidity DESC NULLS FIRST) AS humidity_percent_rank,
        ROUND(running_count_status_bits / SUM(running_count_status_bits) OVER (PARTITION BY device_code), 4) AS running_count_status_bits_share,
        ROW_NUMBER() OVER (ORDER BY running_count_status_bits, id) AS row_seq
    FROM scored
),
remaining_keys AS (
    SELECT id
    FROM metrics
    EXCEPT
    SELECT id
    FROM device_readings
    WHERE firmware = '1.1.0'
),
kept_rows AS (
    SELECT s.*
    FROM metrics s
    JOIN remaining_keys k ON s.id = k.id
),
base AS (
    SELECT
        id,
        humidity,
        status_bits,
        running_count_status_bits,
        temp_c_avg_3,
        humidity_percent_rank,
        row_seq,
        ROUND(row_seq * 1.07, 2) AS row_seq_adjusted
    FROM kept_rows
    WHERE status_bits = 10
)
SELECT
    running_count_status_bits,
    humidity_percent_rank,
    row_seq_adjusted
FROM base;
