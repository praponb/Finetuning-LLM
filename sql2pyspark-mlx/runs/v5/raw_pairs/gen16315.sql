WITH with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        LAG(quantity) OVER (PARTITION BY product_id, store_id ORDER BY last_restock, id) AS prev_quantity,
        ROW_NUMBER() OVER (PARTITION BY product_id, store_id ORDER BY last_restock, id) AS seq_no,
        LEAD(quantity, 2) OVER (PARTITION BY store_id ORDER BY quantity, id) AS next_quantity
    FROM inventory
    WHERE store_id IN (1, 5)
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        product_id,
        prev_quantity,
        next_quantity,
        RANK() OVER w_recent AS prev_quantity_rank,
        ROW_NUMBER() OVER (PARTITION BY product_id, store_id ORDER BY seq_no, id) AS seq_no_2
    FROM with_windows
    WINDOW w_recent AS (PARTITION BY product_id ORDER BY prev_quantity NULLS FIRST)
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        MIN(prev_quantity) OVER (PARTITION BY product_id) AS product_id_min_prev_quantity,
        RANK() OVER w_grp AS next_quantity_rank,
        AVG(prev_quantity_rank) OVER (PARTITION BY product_id ORDER BY seq_no_2, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS prev_quantity_rank_avg_2_prior
    FROM enriched
    WINDOW w_grp AS (PARTITION BY product_id ORDER BY next_quantity DESC NULLS FIRST)
),
base AS (
    -- Keep recent, valid rows
    SELECT *, prev_quantity_rank_avg_2_prior / NULLIF(product_id_min_prev_quantity, 0) AS prev_quantity_rank_avg_2_prior_per_product_id_min_prev_quantity
    FROM scored
    WHERE prev_quantity_rank BETWEEN 6 AND 7
),
scored_inventory AS (
    -- Ranking and gap features
    SELECT
        id,
        prev_quantity,
        prev_quantity_rank,
        prev_quantity_rank_avg_2_prior,
        prev_quantity_rank_avg_2_prior_per_product_id_min_prev_quantity,
        LEAD(prev_quantity_rank_avg_2_prior, 2) OVER (PARTITION BY product_id ORDER BY next_quantity_rank, id) AS next_prev_quantity_rank_avg_2_prior,
        ROUND(prev_quantity_rank / SUM(prev_quantity_rank) OVER (PARTITION BY product_id), 4) AS prev_quantity_rank_share,
        CUME_DIST() OVER (PARTITION BY product_id ORDER BY prev_quantity_rank_avg_2_prior_per_product_id_min_prev_quantity NULLS FIRST) AS prev_quantity_rank_avg_2_prior_per_product_id_min_prev_quantity_cume_dist,
        SUM(prev_quantity_rank_avg_2_prior_per_product_id_min_prev_quantity) OVER (PARTITION BY product_id ORDER BY prev_quantity_rank_avg_2_prior, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_prev_quantity_rank_avg_2_prior_per_product_id_min_prev_quantity,
        LAST_VALUE(seq_no_2, TRUE) OVER (PARTITION BY product_id ORDER BY prev_quantity_rank_avg_2_prior, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_seq_no_2
    FROM base
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN prev_quantity_rank_avg_2_prior_per_product_id_min_prev_quantity >= 74 THEN 'PREMIUM' ELSE 'STANDARD' END AS prev_quantity_rank_avg_2_prior_per_product_id_min_prev_quantity_tier,
    APPROX_PERCENTILE(last_known_seq_no_2, 0.5) AS last_known_seq_no_2_p50,
    MAX(prev_quantity) AS max_prev_quantity,
    COUNT(DISTINCT id) AS distinct_ids
FROM scored_inventory
GROUP BY prev_quantity_rank_avg_2_prior_per_product_id_min_prev_quantity_tier
ORDER BY prev_quantity_rank_avg_2_prior_per_product_id_min_prev_quantity_tier DESC
