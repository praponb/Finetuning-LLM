SELECT
    *,
    ROW_NUMBER() OVER w_part AS event_seq,
    ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY temp_c, id) AS event_seq_2,
    CUME_DIST() OVER (ORDER BY temp_c DESC NULLS FIRST) AS temp_c_cume_dist,
    COUNT(status_bits) OVER w_ordered AS device_code_count_status_bits,
    SUM(status_bits) OVER (PARTITION BY device_code ORDER BY temp_c, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_status_bits
FROM device_readings
WINDOW w_part AS (ORDER BY pressure, id), w_ordered AS (PARTITION BY device_code);
