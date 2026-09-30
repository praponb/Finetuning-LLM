WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        coupon_codes,
        key,
        value
    FROM carts
    LATERAL VIEW EXPLODE(attributes) t AS key, value
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN key = 'weight_kg' THEN 1 ELSE 0 END AS is_weight_kg,
        COALESCE(key, 'Unknown') AS key_filled,
        COALESCE(key, 'N/A') AS key_clean
    FROM exploded
    WHERE key IS NOT NULL
      AND key IN ('gift', 'weight_kg', 'device')
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        customer_id,
        key,
        is_weight_kg,
        SUM(is_weight_kg) OVER (PARTITION BY customer_id ORDER BY is_weight_kg ASC, id ASC, key ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_is_weight_kg,
        DENSE_RANK() OVER (PARTITION BY customer_id ORDER BY is_weight_kg ASC NULLS FIRST) AS is_weight_kg_dense_rank,
        PERCENT_RANK() OVER (PARTITION BY customer_id ORDER BY is_weight_kg ASC NULLS LAST) AS is_weight_kg_percent_rank,
        ROW_NUMBER() OVER (PARTITION BY key ORDER BY is_weight_kg DESC, id ASC, key ASC) AS seq_no,
        SUM(is_weight_kg) OVER (PARTITION BY key ORDER BY is_weight_kg ASC, id ASC, key ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_is_weight_kg_2
    FROM cleaned
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        key,
        is_weight_kg,
        cumulative_is_weight_kg,
        is_weight_kg_dense_rank,
        AVG(is_weight_kg_percent_rank) OVER w_part AS is_weight_kg_percent_rank_avg_5_prior,
        PERCENT_RANK() OVER w_ordered AS is_weight_kg_percent_rank_percent_rank,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY is_weight_kg_percent_rank DESC, id ASC, key ASC) AS row_seq,
        LAST_VALUE(is_weight_kg) IGNORE NULLS OVER (PARTITION BY key ORDER BY is_weight_kg ASC, id ASC, key ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_is_weight_kg
    FROM metrics
    WHERE cumulative_is_weight_kg BETWEEN 357 AND 371
    WINDOW w_part AS (PARTITION BY customer_id ORDER BY cumulative_is_weight_kg_2 ASC, id ASC, key ASC ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING), w_ordered AS (PARTITION BY key, customer_id ORDER BY is_weight_kg_percent_rank ASC NULLS LAST)
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        key,
        is_weight_kg,
        LAG(row_seq) OVER w_hist AS prev_row_seq,
        LEAD(is_weight_kg_percent_rank_percent_rank) OVER w_hist AS next_is_weight_kg_percent_rank_percent_rank,
        LAG(is_weight_kg_percent_rank_percent_rank) OVER w_hist AS prev_is_weight_kg_percent_rank_percent_rank,
        ROW_NUMBER() OVER w_hist AS event_seq,
        LEAD(is_weight_kg, 2, 0) OVER (ORDER BY row_seq ASC, id ASC, key ASC) AS next_is_weight_kg
    FROM with_windows
    WINDOW w_hist AS (PARTITION BY key ORDER BY is_weight_kg_dense_rank ASC, id ASC, key ASC)
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY key ORDER BY next_is_weight_kg_percent_rank_percent_rank DESC NULLS FIRST) AS rank_in_group
        FROM enriched
    ) x
    WHERE rank_in_group = 1
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        key,
        next_is_weight_kg_percent_rank_percent_rank,
        next_is_weight_kg,
        rank_in_group
    FROM deduped
    QUALIFY RANK() OVER (PARTITION BY key ORDER BY prev_row_seq DESC) = 1
)
-- Grouped metrics
SELECT
    key,
    SUM(CASE WHEN key = 'weight_kg' THEN next_is_weight_kg_percent_rank_percent_rank ELSE 0 END) AS weight_kg_next_is_weight_kg_percent_rank_percent_rank,
    MAX(next_is_weight_kg) FILTER (WHERE key = 'utm_source') AS utm_source_max_next_is_weight_kg,
    COUNT(*) FILTER (WHERE key = 'utm_source') AS utm_source_rows,
    SUM(CASE WHEN key = 'gift' THEN next_is_weight_kg ELSE 0 END) AS gift_next_is_weight_kg,
    SIZE(COLLECT_SET(key)) AS num_keys,
    APPROX_PERCENTILE(next_is_weight_kg, 0.9) AS next_is_weight_kg_p90
FROM ranked
GROUP BY CUBE (key)
ORDER BY num_keys DESC NULLS LAST;
