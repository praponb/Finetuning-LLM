WITH enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        temp_c,
        humidity,
        pressure,
        firmware,
        CORR(humidity, pressure) OVER (PARTITION BY device_code) AS corr_humidity_pressure,
        AVG(temp_c) OVER (PARTITION BY device_code ORDER BY temp_c, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS temp_c_avg_3_prior,
        RANK() OVER w_ordered AS status_bits_rank
    FROM device_readings
    WHERE status_bits BETWEEN 7 AND 10
    WINDOW w_ordered AS (PARTITION BY device_code ORDER BY status_bits DESC)
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CASE WHEN pressure >= 1026 THEN 'High' WHEN pressure >= 1000 THEN 'Medium' ELSE 'Low' END AS pressure_tier,
        CEIL(corr_humidity_pressure) AS corr_humidity_pressure_whole
    FROM enriched
    WHERE temp_c > 26
),
enriched_device_readings AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        humidity,
        pressure,
        corr_humidity_pressure,
        status_bits_rank,
        pressure_tier,
        corr_humidity_pressure_whole,
        SUM(temp_c_avg_3_prior) OVER (ORDER BY pressure, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_temp_c_avg_3_prior,
        NTILE(3) OVER (ORDER BY status_bits_rank, id) AS status_bits_rank_bucket,
        SUM(pressure) OVER (ORDER BY corr_humidity_pressure, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_pressure,
        PERCENT_RANK() OVER (ORDER BY corr_humidity_pressure DESC NULLS FIRST) AS corr_humidity_pressure_percent_rank
    FROM valid
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAST_VALUE(pressure, TRUE) OVER (ORDER BY pressure, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_pressure,
        ROW_NUMBER() OVER (ORDER BY pressure, id) AS seq_no,
        LAG(status_bits_rank, 1) OVER (ORDER BY running_temp_c_avg_3_prior, id) AS prev_status_bits_rank,
        SUM(status_bits_rank_bucket) OVER (ORDER BY pressure, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_status_bits_rank_bucket
    FROM enriched_device_readings
),
valid_device_readings AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        humidity,
        pressure,
        corr_humidity_pressure,
        status_bits_rank,
        pressure_tier,
        corr_humidity_pressure_whole,
        running_pressure,
        corr_humidity_pressure_percent_rank,
        prev_status_bits_rank,
        running_status_bits_rank_bucket,
        CASE WHEN running_pressure >= 4134 THEN 'High' WHEN running_pressure >= 1983 THEN 'Medium' ELSE 'Low' END AS running_pressure_tier
    FROM scored
    WHERE running_temp_c_avg_3_prior = 110
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(status_bits_rank) OVER (ORDER BY running_status_bits_rank_bucket, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS status_bits_rank_avg_6,
        SUM(running_status_bits_rank_bucket) OVER (ORDER BY prev_status_bits_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_running_status_bits_rank_bucket
    FROM valid_device_readings
)
SELECT
    id,
    pressure,
    corr_humidity_pressure,
    status_bits_rank,
    pressure_tier,
    corr_humidity_pressure_whole,
    running_pressure,
    running_status_bits_rank_bucket,
    status_bits_rank_avg_6
FROM with_windows;
