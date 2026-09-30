WITH filtered AS (
    -- Keep recent, valid rows
    SELECT *, LENGTH(firmware) AS firmware_length
    FROM device_readings
    WHERE reading_ts >= DATE '2025-09-01'
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        CORR(temp_c, pressure) OVER (PARTITION BY device_code) AS corr_temp_c_pressure,
        ROUND(temp_c / SUM(temp_c) OVER (PARTITION BY device_code), 4) AS temp_c_share,
        CUME_DIST() OVER w_grp AS pressure_cume_dist,
        RANK() OVER (ORDER BY firmware_length) AS firmware_length_rank
    FROM filtered
    WINDOW w_grp AS (PARTITION BY device_code ORDER BY pressure DESC NULLS FIRST)
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(firmware_length_rank) OVER (ORDER BY pressure, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_firmware_length_rank,
        CUME_DIST() OVER (PARTITION BY device_code ORDER BY pressure_cume_dist) AS pressure_cume_dist_cume_dist
    FROM with_windows
),
scored_device_readings AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        STDDEV_SAMP(firmware_length_rank) OVER w_part AS device_code_stddev_samp_firmware_length_rank,
        STDDEV_SAMP(pressure_cume_dist) OVER w_part AS device_code_stddev_samp_pressure_cume_dist
    FROM scored
    WINDOW w_part AS (PARTITION BY device_code)
),
with_windows_device_readings AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        DENSE_RANK() OVER w_ordered AS humidity_dense_rank,
        AVG(device_code_stddev_samp_pressure_cume_dist) OVER w_grp AS device_code_stddev_samp_pressure_cume_dist_avg_2_prior
    FROM scored_device_readings
    WINDOW w_ordered AS (ORDER BY humidity), w_grp AS (PARTITION BY device_code ORDER BY device_code_stddev_samp_firmware_length_rank, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING)
)
SELECT
    id,
    device_code,
    reading_ts,
    temp_c,
    humidity,
    firmware,
    firmware_length,
    corr_temp_c_pressure,
    temp_c_share
FROM with_windows_device_readings;
