WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        device_code,
        humidity,
        pressure,
        status_bits,
        firmware,
        ROUND(pressure * 100, 2) AS adjusted_pressure
    FROM device_readings
    WHERE reading_ts > DATE '2025-09-01'
      AND humidity >= 63
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY device_code ORDER BY adjusted_pressure NULLS FIRST) AS adjusted_pressure_percent_rank,
        ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY humidity DESC, id) AS row_seq,
        RANK() OVER (PARTITION BY device_code ORDER BY status_bits DESC) AS status_bits_rank,
        FIRST_VALUE(adjusted_pressure) IGNORE NULLS OVER (PARTITION BY device_code ORDER BY pressure, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_adjusted_pressure,
        LEAD(adjusted_pressure, 2) OVER (PARTITION BY device_code ORDER BY pressure, id) AS next_adjusted_pressure
    FROM filtered
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        firmware,
        adjusted_pressure,
        status_bits_rank,
        next_adjusted_pressure,
        LAG(row_seq) OVER (PARTITION BY device_code ORDER BY adjusted_pressure_percent_rank, id) AS prev_row_seq,
        LEAD(humidity) OVER (PARTITION BY device_code ORDER BY adjusted_pressure_percent_rank, id) AS next_humidity,
        ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY adjusted_pressure_percent_rank, id) AS row_seq_2,
        RANK() OVER (ORDER BY pressure DESC NULLS FIRST) AS pressure_rank
    FROM metrics
),
metrics_device_readings AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        firmware,
        adjusted_pressure,
        status_bits_rank,
        next_adjusted_pressure,
        next_humidity,
        row_seq_2,
        LAG(pressure_rank) OVER w_recent AS prev_pressure_rank,
        LAG(next_adjusted_pressure) OVER w_recent AS prev_next_adjusted_pressure,
        LEAD(next_adjusted_pressure) OVER w_recent AS next_next_adjusted_pressure,
        ROW_NUMBER() OVER w_recent AS event_seq
    FROM with_windows
    WINDOW w_recent AS (ORDER BY adjusted_pressure, id)
)
SELECT
    id,
    firmware,
    adjusted_pressure,
    status_bits_rank,
    next_adjusted_pressure,
    row_seq_2,
    prev_pressure_rank,
    prev_next_adjusted_pressure,
    event_seq
FROM metrics_device_readings
ORDER BY row_seq_2 DESC, adjusted_pressure;
