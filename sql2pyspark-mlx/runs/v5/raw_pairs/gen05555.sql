WITH metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        device_code,
        temp_c,
        humidity,
        status_bits,
        LEAD(status_bits, 1) OVER (ORDER BY status_bits, id) AS next_status_bits,
        COUNT(status_bits) OVER (PARTITION BY device_code) AS device_code_count_status_bits,
        ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY pressure, id) AS event_seq
    FROM device_readings
    WHERE firmware = '1.1.0'
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY next_status_bits NULLS LAST, id) AS rank_in_group
    FROM metrics
    QUALIFY rank_in_group = 1
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        temp_c,
        next_status_bits,
        rank_in_group,
        LAST_VALUE(event_seq, TRUE) OVER (ORDER BY device_code_count_status_bits, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_seq,
        ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY event_seq, id) AS row_seq
    FROM deduped
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        temp_c,
        rank_in_group,
        last_known_event_seq,
        row_seq,
        FLOOR(last_known_event_seq) AS last_known_event_seq_whole
    FROM scored
    WHERE next_status_bits >= 9
),
valid_device_readings AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN row_seq >= 8 THEN 'High' WHEN row_seq >= 4 THEN 'Medium' ELSE 'Low' END AS row_seq_band,
        FLOOR(rank_in_group) AS rank_in_group_whole
    FROM valid
    WHERE temp_c < 26
)
SELECT
    id,
    temp_c,
    last_known_event_seq,
    row_seq,
    row_seq_band
FROM valid_device_readings
ORDER BY id NULLS FIRST
