WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        status_bits,
        CAST(reading_ts AS DATE) reading_ts_day,
        DATEDIFF(CURRENT_DATE(), reading_ts) days_since_reading_ts
    FROM device_readings
    WHERE temp_c > 25
),
latest AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        status_bits
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY humidity DESC, id) AS dedup_rank
        FROM filtered
    ) t
    WHERE dedup_rank = 1
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        device_code,
        FIRST_VALUE(temp_c, TRUE) OVER (PARTITION BY device_code ORDER BY status_bits, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_temp_c,
        SUM(status_bits) OVER (ORDER BY temp_c, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_status_bits
    FROM latest
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        device_code,
        first_known_temp_c,
        cumulative_status_bits,
        MIN(first_known_temp_c) OVER (PARTITION BY device_code ORDER BY cumulative_status_bits, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) last3_min_first_known_temp_c,
        AVG(first_known_temp_c) OVER (PARTITION BY device_code ORDER BY cumulative_status_bits, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) last3_avg_first_known_temp_c,
        COUNT(first_known_temp_c) OVER (PARTITION BY device_code ORDER BY cumulative_status_bits, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) last3_count_first_known_temp_c,
        ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY cumulative_status_bits, id) row_seq
    FROM enriched
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM with_windows
    QUALIFY RANK() OVER (PARTITION BY device_code ORDER BY row_seq DESC) = 1
),
enriched_device_readings AS (
    -- Ranking and gap features
    SELECT
        id,
        device_code,
        cumulative_status_bits,
        last3_min_first_known_temp_c,
        last3_count_first_known_temp_c,
        row_seq,
        LEAD(cumulative_status_bits) OVER (PARTITION BY device_code ORDER BY last3_count_first_known_temp_c, id) next_cumulative_status_bits,
        LAG(cumulative_status_bits) OVER (PARTITION BY device_code ORDER BY last3_count_first_known_temp_c, id) prev_cumulative_status_bits,
        LEAD(last3_avg_first_known_temp_c) OVER (PARTITION BY device_code ORDER BY last3_count_first_known_temp_c, id) next_last3_avg_first_known_temp_c,
        LAG(last3_avg_first_known_temp_c) OVER (PARTITION BY device_code ORDER BY last3_count_first_known_temp_c, id) prev_last3_avg_first_known_temp_c,
        ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY last3_count_first_known_temp_c, id) event_seq,
        LAST_VALUE(last3_avg_first_known_temp_c, TRUE) OVER (PARTITION BY device_code ORDER BY last3_min_first_known_temp_c, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_last3_avg_first_known_temp_c
    FROM deduped
    WHERE last3_min_first_known_temp_c >= 30
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        device_code,
        last3_min_first_known_temp_c,
        row_seq,
        next_cumulative_status_bits,
        next_last3_avg_first_known_temp_c,
        prev_last3_avg_first_known_temp_c,
        event_seq,
        ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY last3_min_first_known_temp_c, id) event_seq_2,
        AVG(cumulative_status_bits) OVER (PARTITION BY device_code ORDER BY prev_cumulative_status_bits, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) cumulative_status_bits_avg_2,
        SUM(next_last3_avg_first_known_temp_c) OVER (PARTITION BY device_code ORDER BY cumulative_status_bits, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_next_last3_avg_first_known_temp_c,
        LAG(prev_last3_avg_first_known_temp_c, 1) OVER w_running prev_prev_last3_avg_first_known_temp_c
    FROM enriched_device_readings
    WINDOW w_running AS (PARTITION BY device_code ORDER BY event_seq, id)
)
SELECT
    id,
    prev_last3_avg_first_known_temp_c,
    event_seq,
    cumulative_status_bits_avg_2,
    prev_prev_last3_avg_first_known_temp_c
FROM metrics
