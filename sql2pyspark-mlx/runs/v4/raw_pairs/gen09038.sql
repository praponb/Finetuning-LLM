WITH latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY status_bits, id) AS latest_rank
        FROM device_readings
    ) t
    WHERE latest_rank = 1
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(pressure) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_pressure,
        AVG(humidity) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS humidity_avg_2_prior,
        AVG(pressure) OVER (ORDER BY humidity, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS pressure_avg_3_prior
    FROM latest
),
valid AS (
    -- Basic cleansing and defaults
    SELECT *, pressure_avg_3_prior * running_pressure AS pressure_avg_3_prior_times_running_pressure
    FROM metrics
    WHERE reading_ts > DATE '2025-09-01'
),
metrics_device_readings AS (
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
        latest_rank,
        running_pressure,
        humidity_avg_2_prior,
        pressure_avg_3_prior,
        pressure_avg_3_prior_times_running_pressure,
        NTILE(4) OVER (PARTITION BY device_code ORDER BY temp_c DESC, id) AS temp_c_bucket,
        RANK() OVER w_hist AS pressure_avg_3_prior_rank,
        SUM(humidity) OVER w_grp AS device_code_sum_humidity,
        RANK() OVER (PARTITION BY device_code ORDER BY running_pressure DESC NULLS FIRST) AS running_pressure_rank
    FROM valid
    WINDOW w_hist AS (PARTITION BY device_code ORDER BY pressure_avg_3_prior DESC NULLS FIRST), w_grp AS (PARTITION BY device_code)
),
kept_keys AS (
    -- Set difference on keys
    SELECT id
    FROM metrics_device_readings
    EXCEPT
    SELECT id
    FROM device_readings
    WHERE status_bits <= 10
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM metrics_device_readings s
    INNER JOIN kept_keys k ON s.id = k.id
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(status_bits) OVER w_recent AS device_code_sum_status_bits,
        ROUND(pressure_avg_3_prior_rank / SUM(pressure_avg_3_prior_rank) OVER (PARTITION BY device_code), 4) AS pressure_avg_3_prior_rank_share,
        PERCENT_RANK() OVER (PARTITION BY device_code ORDER BY pressure_avg_3_prior_rank) AS pressure_avg_3_prior_rank_percent_rank
    FROM verified
    WINDOW w_recent AS (PARTITION BY device_code)
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        device_code,
        DATE_TRUNC('HOUR', reading_ts) AS hour_window,
        COUNT(*) AS cnt,
        BIT_AND(status_bits) AS status_bits_bit_and,
        COUNT(CASE WHEN device_code = 'DV-04' THEN 1 END) AS dv_04_count,
        COUNT_IF(device_code = 'DV-03') AS dv_03_events,
        PERCENTILE_APPROX(pressure_avg_3_prior_rank_share, ARRAY(0.25, 0.5, 0.75)) AS pressure_avg_3_prior_rank_share_quartiles
    FROM enriched
    GROUP BY device_code, DATE_TRUNC('HOUR', reading_ts)
    HAVING BIT_AND(status_bits) < 1
)
SELECT
    device_code,
    hour_window,
    dv_04_count,
    pressure_avg_3_prior_rank_share_quartiles
FROM grouped
ORDER BY device_code DESC;
