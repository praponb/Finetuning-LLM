WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        device_code,
        humidity,
        pressure,
        status_bits,
        firmware,
        SUBSTRING(device_code, 1, 2) AS device_code_prefix
    FROM device_readings
    WHERE reading_ts > DATE '2025-09-01'
      AND status_bits > 6
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(humidity) OVER (PARTITION BY device_code ORDER BY pressure, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_humidity,
        NTILE(5) OVER (PARTITION BY device_code ORDER BY humidity DESC, id) AS humidity_bucket,
        COUNT(status_bits) OVER (PARTITION BY device_code) AS device_code_count_status_bits,
        NTILE(4) OVER (PARTITION BY device_code ORDER BY pressure DESC, id) AS pressure_bucket,
        AVG(pressure) OVER (PARTITION BY device_code ORDER BY status_bits, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS pressure_avg_3_prior
    FROM valid
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        device_code,
        humidity,
        pressure,
        status_bits,
        firmware,
        device_code_prefix,
        cumulative_humidity,
        humidity_bucket,
        device_code_count_status_bits,
        pressure_bucket,
        pressure_avg_3_prior,
        NTILE(5) OVER (PARTITION BY device_code ORDER BY device_code_count_status_bits DESC, id) AS device_code_count_status_bits_bucket,
        AVG(pressure_avg_3_prior) OVER w_running AS pressure_avg_3_prior_avg_5_prior
    FROM metrics
    WINDOW w_running AS (PARTITION BY device_code ORDER BY humidity, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING)
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY pressure_bucket DESC, id) AS rank_in_group
        FROM scored
    ) t
    WHERE rank_in_group = 1
),
metrics_device_readings AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(pressure_avg_3_prior / SUM(pressure_avg_3_prior) OVER (PARTITION BY device_code), 4) AS pressure_avg_3_prior_share,
        AVG(pressure) OVER (ORDER BY status_bits, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS pressure_avg_5
    FROM deduped
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY device_code_count_status_bits DESC NULLS LAST, id) AS dedup_rank
        FROM metrics_device_readings
    ) t
    WHERE dedup_rank = 1
),
base AS (
    -- Keep recent, valid rows
    SELECT *, INITCAP(TRIM(device_code)) AS device_code_title
    FROM ranked
    WHERE pressure_avg_3_prior_avg_5_prior < 1026
),
wide AS (
    -- Native SQL PIVOT
    SELECT *
    FROM (
        SELECT
            firmware,
            device_code,
            pressure_bucket
        FROM base
    ) src
    PIVOT (
        AVG(pressure_bucket) AS value_sum
        FOR device_code IN ('DV-01' AS dv_01, 'DV-03' AS dv_03, 'DV-04' AS dv_04)
    )
)
SELECT
    firmware,
    dv_03,
    dv_04
FROM wide
ORDER BY firmware DESC, dv_03 DESC;
