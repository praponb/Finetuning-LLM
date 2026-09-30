WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        reading_ts,
        temp_c,
        status_bits,
        ROUND(humidity * 1.15, 2) adjusted_humidity
    FROM device_readings
    WHERE reading_ts >= TIMESTAMP '2025-09-01 02:00:00'
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        RANK() OVER (ORDER BY adjusted_humidity DESC) adjusted_humidity_rank,
        ROW_NUMBER() OVER (ORDER BY status_bits DESC, id) event_seq
    FROM valid
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        reading_ts,
        event_seq,
        LEAD(temp_c, 2) OVER (ORDER BY adjusted_humidity, id) next_temp_c,
        SUM(status_bits) OVER (ORDER BY adjusted_humidity_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_status_bits,
        SUM(event_seq) OVER w_part event_seq_sum_15m,
        LAG(event_seq, 2, 0) OVER (ORDER BY event_seq, id) prev_event_seq
    FROM scored
    WHERE temp_c BETWEEN 35 AND 36
    WINDOW w_part AS (ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW)
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(next_temp_c) OVER (ORDER BY reading_ts, id) next_next_temp_c,
        LEAD(prev_event_seq) OVER (ORDER BY reading_ts, id) next_prev_event_seq,
        LAG(prev_event_seq) OVER (ORDER BY reading_ts, id) prev_prev_event_seq,
        ROW_NUMBER() OVER (ORDER BY reading_ts, id) event_seq_2,
        ROW_NUMBER() OVER (ORDER BY event_seq, id) event_seq_3
    FROM metrics
)
SELECT
    id,
    reading_ts,
    event_seq,
    next_temp_c,
    cumulative_status_bits,
    event_seq_sum_15m,
    prev_event_seq,
    next_next_temp_c,
    next_prev_event_seq
FROM enriched
