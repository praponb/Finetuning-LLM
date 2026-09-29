WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        effective_from,
        val
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(discounts) x AS val
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        PERCENT_RANK() OVER (ORDER BY val) AS val_percent_rank,
        PERCENT_RANK() OVER (ORDER BY val NULLS LAST) AS val_percent_rank_2,
        PERCENT_RANK() OVER (ORDER BY val DESC NULLS FIRST) AS val_percent_rank_3,
        CUME_DIST() OVER (ORDER BY val NULLS FIRST) AS val_cume_dist,
        PERCENT_RANK() OVER (ORDER BY val DESC NULLS FIRST) AS val_percent_rank_4
    FROM flattened
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        effective_from,
        val,
        val_percent_rank_2,
        val_percent_rank_3,
        val_cume_dist,
        val_percent_rank_4,
        COALESCE(val, 0) AS val_filled,
        val_percent_rank_2 / NULLIF(val, 0) AS val_percent_rank_2_per_val
    FROM scored
    WHERE effective_from > DATE '2024-02-20'
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        val,
        val_percent_rank_2,
        val_percent_rank_3,
        val_cume_dist,
        val_percent_rank_2_per_val,
        CUME_DIST() OVER w_running AS val_percent_rank_4_cume_dist,
        PERCENT_RANK() OVER (ORDER BY val_percent_rank_4) AS val_percent_rank_4_percent_rank
    FROM base
    WINDOW w_running AS (ORDER BY val_percent_rank_4 DESC)
),
enriched_price_tiers AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        PERCENT_RANK() OVER (ORDER BY val_percent_rank_4_percent_rank DESC NULLS FIRST) AS val_percent_rank_4_percent_rank_percent_rank,
        DENSE_RANK() OVER (ORDER BY val_percent_rank_2 NULLS FIRST) AS val_percent_rank_2_dense_rank,
        RANK() OVER (ORDER BY val_percent_rank_3 DESC NULLS LAST) AS val_percent_rank_3_rank,
        DENSE_RANK() OVER (ORDER BY val_percent_rank_2_per_val DESC NULLS LAST) AS val_percent_rank_2_per_val_dense_rank
    FROM enriched
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        val_percent_rank_2 * val_percent_rank_3 AS val_percent_rank_2_times_val_percent_rank_3,
        val_percent_rank_3_rank + val_percent_rank_4_percent_rank AS val_percent_rank_3_rank_plus_val_percent_rank_4_percent_rank
    FROM enriched_price_tiers
    WHERE val_percent_rank_3_rank != 5
),
base_price_tiers AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CAST(val_percent_rank_3 AS INT) AS val_percent_rank_3_int
    FROM cleaned
    WHERE val <= 62
)
SELECT
    val,
    val_percent_rank_2,
    val_percent_rank_3,
    val_percent_rank_2_per_val,
    val_percent_rank_4_cume_dist,
    val_percent_rank_4_percent_rank_percent_rank,
    val_percent_rank_2_dense_rank,
    val_percent_rank_3_rank,
    val_percent_rank_2_per_val_dense_rank
FROM base_price_tiers
ORDER BY val_percent_rank_2 DESC NULLS LAST;
