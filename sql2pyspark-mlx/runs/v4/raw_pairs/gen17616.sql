WITH lines AS (
    SELECT
        id,
        created_at,
        coupon_codes,
        key,
        attr_value
    FROM carts
    LATERAL VIEW EXPLODE(attributes) li AS key, attr_value
),
with_windows AS (
    SELECT
        *,
        ROW_NUMBER() OVER w_part AS seq_no,
        ROW_NUMBER() OVER w_part AS seq_no_2
    FROM lines
    WINDOW w_part AS (PARTITION BY key ORDER BY created_at, id, key)
),
with_gaps AS (
    SELECT *, CASE WHEN LAG(created_at) OVER (PARTITION BY key ORDER BY created_at, id, key) IS NULL
                 OR created_at > LAG(created_at) OVER (PARTITION BY key ORDER BY created_at, id, key) + INTERVAL '45' MINUTE THEN 1 ELSE 0 END AS new_group_flag
    FROM with_windows
),
sessionized AS (
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY key ORDER BY created_at, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM with_gaps
),
scored AS (
    SELECT
        *,
        SUM(new_group_flag) OVER (PARTITION BY key ORDER BY created_at, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_new_group_flag,
        SUM(seq_no_2) OVER (PARTITION BY key ORDER BY seq_no, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_seq_no_2,
        LEAD(island_id, 2, 0) OVER (PARTITION BY key ORDER BY new_group_flag, id, key) AS next_island_id,
        PERCENT_RANK() OVER (PARTITION BY key ORDER BY island_id) AS island_id_percent_rank,
        LAG(seq_no_2, 1) OVER (ORDER BY new_group_flag, id, key) AS prev_seq_no_2
    FROM sessionized
),
metrics AS (
    SELECT
        id,
        created_at,
        coupon_codes,
        key,
        attr_value,
        seq_no,
        seq_no_2,
        new_group_flag,
        island_id,
        running_new_group_flag,
        cumulative_seq_no_2,
        next_island_id,
        island_id_percent_rank,
        prev_seq_no_2,
        LEAD(cumulative_seq_no_2, 1) OVER (PARTITION BY key ORDER BY cumulative_seq_no_2, id, key) AS next_cumulative_seq_no_2,
        FIRST_VALUE(next_island_id) IGNORE NULLS OVER (PARTITION BY key ORDER BY new_group_flag, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_next_island_id,
        FIRST_VALUE(cumulative_seq_no_2) IGNORE NULLS OVER (PARTITION BY key ORDER BY seq_no, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_cumulative_seq_no_2,
        LAST_VALUE(key, TRUE) OVER (PARTITION BY key ORDER BY island_id_percent_rank, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_key,
        SUM(prev_seq_no_2) OVER (PARTITION BY key ORDER BY seq_no, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_prev_seq_no_2
    FROM scored
),
metrics_carts AS (
    SELECT
        id,
        created_at,
        coupon_codes,
        key,
        attr_value,
        seq_no,
        seq_no_2,
        new_group_flag,
        island_id,
        running_new_group_flag,
        cumulative_seq_no_2,
        next_island_id,
        island_id_percent_rank,
        prev_seq_no_2,
        next_cumulative_seq_no_2,
        first_known_next_island_id,
        first_known_cumulative_seq_no_2,
        last_known_key,
        cumulative_prev_seq_no_2,
        SUM(seq_no) OVER w_hist AS seq_no_sum_15m,
        SUM(seq_no_2) OVER (PARTITION BY key) AS key_sum_seq_no_2
    FROM metrics
    WINDOW w_hist AS (PARTITION BY key ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW)
)
SELECT
    CASE WHEN cumulative_prev_seq_no_2 >= 32 THEN 'PREMIUM' ELSE 'STANDARD' END AS cumulative_prev_seq_no_2_tier,
    CASE WHEN new_group_flag >= 1 THEN 'PREMIUM' ELSE 'STANDARD' END AS new_group_flag_tier,
    CASE WHEN first_known_next_island_id >= 8 THEN 'LARGE' ELSE 'SMALL' END AS first_known_next_island_id_tier,
    ROUND(AVG(next_island_id), 2) AS avg_next_island_id,
    MAX(new_group_flag) FILTER (WHERE key = 'weight_kg') AS weight_kg_max_new_group_flag,
    COUNT(DISTINCT id) AS distinct_ids,
    ROUND(SUM(island_id_percent_rank) / NULLIF(COUNT(CASE WHEN key = 'weight_kg' THEN 1 END), 0), 2) AS island_id_percent_rank_per_weight_kg,
    PERCENTILE_APPROX(island_id, 0.9) AS island_id_p90
FROM metrics_carts
GROUP BY ROLLUP (CASE WHEN cumulative_prev_seq_no_2 >= 32 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN new_group_flag >= 1 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN first_known_next_island_id >= 8 THEN 'LARGE' ELSE 'SMALL' END)
HAVING island_id_percent_rank_per_weight_kg >= 0
ORDER BY island_id_p90 DESC NULLS LAST;
