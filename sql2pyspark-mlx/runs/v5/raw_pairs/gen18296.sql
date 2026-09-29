WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        device_code,
        temp_c,
        humidity,
        status_bits,
        firmware,
        CASE WHEN device_code IN ('DV-04', 'DV-02') THEN 'Priority' ELSE 'Other' END device_code_group
    FROM device_readings
    WHERE device_code NOT IN ('DV-02', 'DV-04')
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        temp_c,
        status_bits,
        device_code_group,
        ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY humidity DESC, id) event_seq,
        RANK() OVER (PARTITION BY device_code ORDER BY status_bits) status_bits_rank,
        RANK() OVER (PARTITION BY device_code ORDER BY temp_c DESC) temp_c_rank,
        STDDEV_SAMP(temp_c) OVER w_recent device_code_stddev_samp_temp_c
    FROM base
    WHERE firmware = '2.0.1'
    WINDOW w_recent AS (PARTITION BY device_code)
),
metrics_device_readings AS (
    -- Ranking and gap features
    SELECT
        id,
        temp_c,
        status_bits,
        device_code_group,
        event_seq,
        status_bits_rank,
        FIRST_VALUE(event_seq, TRUE) OVER (ORDER BY device_code_stddev_samp_temp_c, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_event_seq,
        LEAD(temp_c, 2) OVER (ORDER BY temp_c, id) next_temp_c
    FROM metrics
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        COUNT(first_known_event_seq) OVER w_recent running_count_first_known_event_seq,
        MIN(first_known_event_seq) OVER w_recent running_min_first_known_event_seq,
        MAX(first_known_event_seq) OVER w_recent running_max_first_known_event_seq,
        LAG(next_temp_c, 1, 0) OVER w_ordered prev_next_temp_c
    FROM metrics_device_readings
    WINDOW w_recent AS (ORDER BY first_known_event_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_ordered AS (ORDER BY status_bits_rank, id)
)
SELECT
    id,
    temp_c,
    status_bits,
    device_code_group,
    event_seq,
    next_temp_c,
    running_count_first_known_event_seq,
    running_max_first_known_event_seq,
    prev_next_temp_c
FROM enriched
ORDER BY running_count_first_known_event_seq
