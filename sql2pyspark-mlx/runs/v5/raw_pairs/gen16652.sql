WITH valid AS (
    SELECT *, ROUND(pressure / temp_c, 2) AS pressure_per_temp_c
    FROM device_readings
    WHERE reading_ts >= TIMESTAMP '2025-09-01 07:00:00'
      AND temp_c BETWEEN 28 AND 32
),
enriched AS (
    SELECT
        id,
        reading_ts,
        temp_c,
        pressure,
        status_bits,
        firmware,
        LEAD(humidity) OVER w_grp AS next_humidity,
        LAG(pressure) OVER w_grp AS prev_pressure,
        ROW_NUMBER() OVER w_grp AS event_seq
    FROM valid
    WHERE temp_c <= 27
    WINDOW w_grp AS (PARTITION BY device_code ORDER BY humidity ASC, id ASC)
),
scored AS (
    SELECT
        *,
        SUM(pressure) OVER (ORDER BY temp_c ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_pressure,
        LEAD(event_seq, 1) OVER w_part AS next_event_seq
    FROM enriched
    WINDOW w_part AS (ORDER BY reading_ts ASC, id ASC)
),
scored_device_readings AS (
    SELECT
        *,
        AVG(next_humidity) OVER (ORDER BY temp_c ASC, id ASC ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS next_humidity_avg_5_prior,
        LAST_VALUE(event_seq) IGNORE NULLS OVER (ORDER BY event_seq ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_seq,
        LAST_VALUE(event_seq) IGNORE NULLS OVER (ORDER BY prev_pressure ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_seq_2,
        AVG(status_bits) OVER (ORDER BY CAST(reading_ts AS BIGINT) ASC RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS status_bits_avg_15m,
        SUM(pressure) OVER (ORDER BY event_seq ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_pressure_2
    FROM scored
),
metrics AS (
    SELECT
        id,
        status_bits,
        firmware,
        prev_pressure,
        event_seq,
        next_event_seq,
        last_known_event_seq_2,
        running_pressure_2,
        AVG(status_bits) OVER (ORDER BY running_pressure ASC, id ASC ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS status_bits_avg_5,
        SUM(last_known_event_seq) OVER (ORDER BY CAST(reading_ts AS BIGINT) ASC RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS last_known_event_seq_sum_15m
    FROM scored_device_readings
),
metrics_device_readings AS (
    SELECT
        id,
        status_bits,
        event_seq,
        next_event_seq,
        last_known_event_seq_2,
        last_known_event_seq_sum_15m,
        NTILE(10) OVER w_running AS event_seq_bucket,
        ROW_NUMBER() OVER w_recent AS seq_no,
        AVG(status_bits) OVER (ORDER BY event_seq ASC, id ASC ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS status_bits_avg_6_prior
    FROM metrics
    WINDOW w_running AS (ORDER BY event_seq ASC, id ASC), w_recent AS (ORDER BY last_known_event_seq_sum_15m ASC, id ASC)
),
valid_device_readings AS (
    SELECT *, ROUND(status_bits / last_known_event_seq_2, 2) AS status_bits_per_last_known_event_seq_2
    FROM metrics_device_readings
    WHERE last_known_event_seq_2 BETWEEN 3 AND 6
),
stats AS (
    SELECT
        CASE WHEN event_seq >= 7 THEN 'LARGE' ELSE 'SMALL' END AS event_seq_tier,
        seq_no,
        ROUND(AVG(event_seq_bucket), 2) AS avg_event_seq_bucket,
        COUNT(*) AS num_rows,
        GROUPING(CASE WHEN event_seq >= 7 THEN 'LARGE' ELSE 'SMALL' END) AS gid
    FROM valid_device_readings
    GROUP BY GROUPING SETS ((CASE WHEN event_seq >= 7 THEN 'LARGE' ELSE 'SMALL' END, seq_no), (CASE WHEN event_seq >= 7 THEN 'LARGE' ELSE 'SMALL' END), ())
    HAVING ROUND(AVG(event_seq_bucket), 2) > 0
)
SELECT
    event_seq_tier,
    seq_no,
    avg_event_seq_bucket,
    gid
FROM stats
