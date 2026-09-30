WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        coupon_codes,
        k,
        attr_value
    FROM carts
    LATERAL VIEW EXPLODE(attributes) e AS k, attr_value
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        coupon_codes,
        k,
        CASE WHEN k IN ('weight_kg', 'utm_source') THEN 'Tier 1' ELSE 'Other' END k_group
    FROM exploded
    WHERE customer_id IS NOT NULL
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        LENGTH(k) k_length,
        SIZE(coupon_codes) coupon_codes_count,
        COALESCE(k, 'UNKNOWN') k_clean
    FROM filtered
    WHERE k NOT IN ('gift', 'device')
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        FIRST_VALUE(coupon_codes_count, TRUE) OVER (PARTITION BY k ORDER BY coupon_codes_count, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_coupon_codes_count,
        LEAD(k_length, 1) OVER w_hist next_k_length,
        AVG(coupon_codes_count) OVER w_hist_2 coupon_codes_count_avg_6,
        ROW_NUMBER() OVER (PARTITION BY k ORDER BY coupon_codes_count DESC, id, k) seq_no,
        SUM(k_length) OVER w_ordered k_sum_k_length
    FROM valid
    WINDOW w_hist AS (PARTITION BY k ORDER BY coupon_codes_count, id, k), w_hist_2 AS (PARTITION BY k ORDER BY k_length, id, k ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), w_ordered AS (PARTITION BY k)
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY k ORDER BY seq_no DESC, id, k) AS latest_rank
    FROM with_windows
    QUALIFY latest_rank = 1
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        DENSE_RANK() OVER w_running coupon_codes_count_dense_rank,
        CUME_DIST() OVER (ORDER BY k_sum_k_length NULLS LAST) k_sum_k_length_cume_dist,
        LAG(seq_no, 2) OVER w_running_2 prev_seq_no,
        SUM(k_length) OVER (ORDER BY first_known_coupon_codes_count, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_k_length
    FROM latest
    WINDOW w_running AS (PARTITION BY k ORDER BY coupon_codes_count NULLS FIRST), w_running_2 AS (ORDER BY latest_rank, id, k)
),
stats AS (
    -- Multi-level aggregation
    SELECT
        prev_seq_no,
        COUNT(DISTINCT id) AS distinct_ids,
        SUM(CASE WHEN k = 'gift' THEN k_length ELSE 0 END) AS gift_k_length
    FROM scored
    GROUP BY prev_seq_no
)
SELECT prev_seq_no, gift_k_length
FROM stats
