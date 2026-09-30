WITH flattened AS (
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        key,
        value
    FROM carts
    LATERAL VIEW EXPLODE(attributes) x AS key, value
),
lines AS (
    SELECT
        id,
        customer_id,
        key,
        value,
        EXPLODE(coupon_codes) AS value_2
    FROM flattened
),
valid AS (
    SELECT
        id,
        customer_id,
        key,
        value,
        value_2,
        CASE WHEN key = 'weight_kg' THEN 1 ELSE 0 END AS is_weight_kg,
        INITCAP(TRIM(value_2)) AS value_2_title,
        COALESCE(value_2, 'OTHER') AS resolved_value_2
    FROM lines
    WHERE value_2 = 'BUNDLE'
),
metrics AS (
    SELECT
        id,
        key,
        is_weight_kg,
        SUM(is_weight_kg) OVER (PARTITION BY customer_id) AS customer_id_sum_is_weight_kg,
        PERCENT_RANK() OVER (PARTITION BY customer_id, key ORDER BY is_weight_kg DESC NULLS LAST) AS is_weight_kg_percent_rank,
        AVG(is_weight_kg) OVER w_hist AS value_2_avg_is_weight_kg
    FROM valid
    WINDOW w_hist AS (PARTITION BY value_2, key)
),
metrics_carts AS (
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY key ORDER BY is_weight_kg) AS is_weight_kg_dense_rank,
        DENSE_RANK() OVER (PARTITION BY key ORDER BY value_2_avg_is_weight_kg) AS value_2_avg_is_weight_kg_dense_rank
    FROM metrics
    WHERE customer_id_sum_is_weight_kg < 246
),
totals AS (
    SELECT
        CASE WHEN is_weight_kg_dense_rank >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END AS is_weight_kg_dense_rank_tier,
        CASE WHEN value_2_avg_is_weight_kg_dense_rank >= 9 THEN 'HIGH' ELSE 'LOW' END AS value_2_avg_is_weight_kg_dense_rank_tier,
        COUNT(*) FILTER (WHERE key = 'gift') AS gift_rows,
        COUNT_IF(key = 'gift') AS gift_events,
        SORT_ARRAY(COLLECT_SET(key)) AS key_list,
        SORT_ARRAY(COLLECT_SET(key)) AS key_list_2,
        DENSE_RANK() OVER (PARTITION BY CASE WHEN is_weight_kg_dense_rank >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END ORDER BY COUNT_IF(key = 'gift') DESC) AS gift_events_dense_rank
    FROM metrics_carts
    GROUP BY CASE WHEN is_weight_kg_dense_rank >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN value_2_avg_is_weight_kg_dense_rank >= 9 THEN 'HIGH' ELSE 'LOW' END
)
SELECT
    is_weight_kg_dense_rank_tier,
    value_2_avg_is_weight_kg_dense_rank_tier,
    gift_rows,
    key_list,
    gift_events_dense_rank
FROM totals
ORDER BY gift_rows NULLS FIRST, value_2_avg_is_weight_kg_dense_rank_tier DESC
