WITH scored AS (
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
        ROUND(temp_c / SUM(temp_c) OVER (PARTITION BY device_code), 4) AS temp_c_share,
        LEAD(humidity, 2) OVER (PARTITION BY device_code ORDER BY pressure ASC, id ASC) AS next_humidity,
        LAST_VALUE(humidity, TRUE) OVER (PARTITION BY device_code ORDER BY humidity ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_humidity,
        ROW_NUMBER() OVER (ORDER BY temp_c DESC, id ASC) AS seq_no,
        PERCENT_RANK() OVER (ORDER BY temp_c DESC NULLS LAST) AS temp_c_percent_rank
    FROM device_readings
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        LEAST(pressure, temp_c_share) AS least_pressure_temp_c_share,
        CASE WHEN firmware = '2.0.1' THEN 1 ELSE 0 END AS is_2_0_1
    FROM scored
    WHERE reading_ts > DATE '2025-09-01'
      AND last_known_humidity BETWEEN 63 AND 65
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        CORR(pressure, temp_c) OVER w_running AS corr_pressure_temp_c,
        NTILE(4) OVER w_running_2 AS humidity_bucket,
        FIRST_VALUE(temp_c) IGNORE NULLS OVER (PARTITION BY device_code ORDER BY least_pressure_temp_c_share ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_temp_c,
        DENSE_RANK() OVER (PARTITION BY device_code ORDER BY seq_no DESC) AS seq_no_dense_rank,
        CORR(humidity, temp_c) OVER w_running AS corr_humidity_temp_c
    FROM cleaned
    WINDOW w_running AS (PARTITION BY device_code), w_running_2 AS (ORDER BY humidity ASC, id ASC)
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY first_known_temp_c DESC NULLS FIRST, id ASC) AS row_num
        FROM with_windows
    ) t
    WHERE row_num = 1
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(corr_pressure_temp_c) OVER w_ordered AS device_code_sum_corr_pressure_temp_c,
        COUNT(seq_no_dense_rank) OVER w_recent AS seq_no_dense_rank_count_2h,
        LAG(is_2_0_1, 1) OVER (PARTITION BY device_code ORDER BY seq_no_dense_rank ASC, id ASC) AS prev_is_2_0_1
    FROM ranked
    WINDOW w_ordered AS (PARTITION BY device_code), w_recent AS (PARTITION BY device_code ORDER BY CAST(reading_ts AS BIGINT) ASC RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW)
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        temp_c_share,
        next_humidity,
        last_known_humidity,
        seq_no,
        temp_c_percent_rank,
        least_pressure_temp_c_share,
        is_2_0_1,
        corr_pressure_temp_c,
        humidity_bucket,
        first_known_temp_c,
        seq_no_dense_rank,
        corr_humidity_temp_c,
        row_num,
        device_code_sum_corr_pressure_temp_c,
        seq_no_dense_rank_count_2h,
        prev_is_2_0_1,
        SUM(temp_c_percent_rank) OVER (PARTITION BY device_code ORDER BY corr_humidity_temp_c ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_temp_c_percent_rank,
        LAST_VALUE(least_pressure_temp_c_share) IGNORE NULLS OVER (PARTITION BY device_code ORDER BY status_bits ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_least_pressure_temp_c_share,
        NTILE(10) OVER (PARTITION BY device_code ORDER BY prev_is_2_0_1 DESC, id ASC) AS prev_is_2_0_1_bucket
    FROM metrics
),
kept_keys AS (
    -- Set difference on keys
    SELECT device_code, id
    FROM enriched
    EXCEPT DISTINCT
    SELECT device_code, id
    FROM device_readings
    WHERE firmware = '1.0.3'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    INNER JOIN kept_keys k ON s.device_code = k.device_code AND s.id = k.id
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        device_code,
        COUNT(*) AS cnt,
        ROUND(SUM(prev_is_2_0_1_bucket) / NULLIF(COUNT(CASE WHEN device_code = 'DV-01' THEN 1 END), 0), 2) AS prev_is_2_0_1_bucket_per_dv_01,
        COUNT(*) FILTER (WHERE device_code = 'DV-03') AS dv_03_rows
    FROM kept_rows
    GROUP BY device_code
)
SELECT
    device_code,
    cnt,
    prev_is_2_0_1_bucket_per_dv_01
FROM summary
