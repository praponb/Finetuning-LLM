WITH flattened AS (
    SELECT
        id,
        level,
        status_code,
        key,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) li AS key, attr_value
),
unpacked AS (
    SELECT
        id,
        level,
        status_code,
        key,
        CAST((status_code & 4) != 0 AS INT) AS flag_bit4,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM flattened
    WHERE status_code = 305
),
metrics AS (
    SELECT
        id,
        status_code,
        STDDEV_SAMP(status_code_high) OVER (PARTITION BY key) AS key_stddev_samp_status_code_high,
        CUME_DIST() OVER (PARTITION BY level, key ORDER BY flag_bit4 ASC NULLS LAST) AS flag_bit4_cume_dist,
        DENSE_RANK() OVER (PARTITION BY level ORDER BY status_code DESC) AS status_code_dense_rank,
        RANK() OVER (ORDER BY status_code ASC) AS status_code_rank,
        RANK() OVER (PARTITION BY level ORDER BY flag_bit4 DESC NULLS FIRST) AS flag_bit4_rank
    FROM unpacked
),
filtered AS (
    SELECT
        id,
        status_code,
        flag_bit4_cume_dist,
        status_code_dense_rank,
        status_code_rank,
        flag_bit4_rank,
        FLOOR(status_code_dense_rank) AS status_code_dense_rank_whole,
        status_code - flag_bit4_cume_dist AS status_code_minus_flag_bit4_cume_dist
    FROM metrics
    WHERE flag_bit4_rank <> 5
),
cleaned AS (
    SELECT
        id,
        status_code,
        flag_bit4_cume_dist,
        status_code_rank,
        flag_bit4_rank,
        status_code_dense_rank_whole,
        LEAST(status_code_dense_rank, flag_bit4_rank) AS least_status_code_dense_rank_flag_bit4_rank
    FROM filtered
    WHERE flag_bit4_rank = 8
),
enriched AS (
    SELECT *, DENSE_RANK() OVER (ORDER BY least_status_code_dense_rank_flag_bit4_rank DESC) AS least_status_code_dense_rank_flag_bit4_rank_dense_rank
    FROM cleaned
    WHERE flag_bit4_cume_dist <= 0.7
),
cleaned_app_logs AS (
    SELECT
        id,
        status_code,
        status_code_rank,
        flag_bit4_rank,
        status_code_dense_rank_whole,
        least_status_code_dense_rank_flag_bit4_rank,
        least_status_code_dense_rank_flag_bit4_rank_dense_rank,
        status_code_rank + least_status_code_dense_rank_flag_bit4_rank_dense_rank AS status_code_rank_plus_least_status_code_dense_rank_flag_bit4_rank_dense_rank,
        flag_bit4_rank * least_status_code_dense_rank_flag_bit4_rank AS flag_bit4_rank_times_least_status_code_dense_rank_flag_bit4_rank
    FROM enriched
    WHERE flag_bit4_cume_dist > 0.51
),
grouped AS (
    SELECT
        CASE WHEN status_code_rank >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END AS status_code_rank_tier,
        ROUND(AVG(least_status_code_dense_rank_flag_bit4_rank_dense_rank), 2) AS avg_least_status_code_dense_rank_flag_bit4_rank_dense_rank,
        APPROX_PERCENTILE(flag_bit4_rank, 0.9) AS flag_bit4_rank_p90,
        COUNT(*) AS event_count,
        APPROX_PERCENTILE(flag_bit4_rank, ARRAY(0.25, 0.5, 0.75)) AS flag_bit4_rank_quartiles,
        ROUND(AVG(flag_bit4_rank_times_least_status_code_dense_rank_flag_bit4_rank), 2) AS avg_flag_bit4_rank_times_least_status_code_dense_rank_flag_bit4_rank,
        BIT_AND(status_code) AS status_code_bit_and
    FROM cleaned_app_logs
    GROUP BY 1
)
SELECT
    status_code_rank_tier,
    avg_least_status_code_dense_rank_flag_bit4_rank_dense_rank,
    flag_bit4_rank_p90,
    event_count,
    avg_flag_bit4_rank_times_least_status_code_dense_rank_flag_bit4_rank
FROM grouped
ORDER BY event_count DESC NULLS LAST
