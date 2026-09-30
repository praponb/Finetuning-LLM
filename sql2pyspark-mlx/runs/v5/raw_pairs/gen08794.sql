WITH scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        firmware,
        ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY status_bits, id) AS seq_no,
        LAG(pressure, 1) OVER (PARTITION BY device_code ORDER BY humidity, id) AS prev_pressure
    FROM device_readings
    WHERE status_bits >= 6
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, COALESCE(prev_pressure, 0) AS prev_pressure_filled
    FROM scored
    WHERE prev_pressure < 1021
),
matched_keys AS (
    -- Set difference on keys
    SELECT firmware, id
    FROM cleaned
    EXCEPT
    SELECT firmware, id
    FROM device_readings
    WHERE humidity > 50
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM cleaned s
    INNER JOIN matched_keys k ON s.firmware = k.firmware AND s.id = k.id
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        PERCENT_RANK() OVER (ORDER BY seq_no DESC NULLS FIRST) AS seq_no_percent_rank,
        PERCENT_RANK() OVER w_grp AS prev_pressure_percent_rank
    FROM reconciled
    WINDOW w_grp AS (ORDER BY prev_pressure NULLS FIRST)
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        firmware,
        seq_no,
        prev_pressure,
        prev_pressure_percent_rank,
        RANK() OVER (ORDER BY prev_pressure DESC NULLS LAST) AS prev_pressure_rank
    FROM with_windows
)
SELECT
    firmware,
    seq_no,
    prev_pressure_rank
FROM metrics
ORDER BY firmware NULLS FIRST, prev_pressure_rank
