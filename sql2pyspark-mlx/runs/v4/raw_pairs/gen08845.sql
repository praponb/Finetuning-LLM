WITH deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY device_code ORDER BY humidity NULLS LAST) AS row_num
        FROM device_readings
    ) ranked
    WHERE row_num = 1
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        CORR(humidity, temp_c) OVER w_grp AS corr_humidity_temp_c,
        STDDEV_SAMP(humidity) OVER w_grp AS device_code_stddev_samp_humidity,
        DENSE_RANK() OVER (ORDER BY status_bits) AS status_bits_dense_rank
    FROM deduped
    WINDOW w_grp AS (PARTITION BY device_code)
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY row_num DESC, id) AS row_num_2
    FROM metrics
    QUALIFY row_num_2 = 1
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(status_bits_dense_rank, 2) OVER (PARTITION BY device_code ORDER BY pressure, id) AS prev_status_bits_dense_rank,
        CUME_DIST() OVER (PARTITION BY device_code ORDER BY status_bits_dense_rank DESC NULLS LAST) AS status_bits_dense_rank_cume_dist,
        ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY row_num, id) AS row_seq
    FROM latest
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        firmware,
        device_code,
        status_bits_dense_rank_cume_dist
    FROM with_windows
) src
PIVOT (
    AVG(status_bits_dense_rank_cume_dist) AS total
    FOR device_code IN ('DV-01' AS dv_01, 'DV-04' AS dv_04)
)
