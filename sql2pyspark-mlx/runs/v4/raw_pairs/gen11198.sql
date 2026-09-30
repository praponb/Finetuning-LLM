WITH ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY starts_on DESC, id) AS latest_rank
        FROM promotions
    ) x
    WHERE latest_rank = 1
),
ranked_promotions AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY ends_on, id) AS latest_rank_2
    FROM ranked
    QUALIFY latest_rank_2 = 1
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(discount_pct, 2) OVER (ORDER BY starts_on, id) next_discount_pct,
        NTILE(3) OVER (PARTITION BY promo_code, channel ORDER BY discount_pct, id) discount_pct_bucket,
        RANK() OVER (ORDER BY discount_pct) discount_pct_rank,
        DENSE_RANK() OVER (PARTITION BY channel ORDER BY discount_pct DESC) discount_pct_dense_rank,
        LAG(discount_pct, 2, 0) OVER (PARTITION BY promo_code, channel ORDER BY latest_rank, id) prev_discount_pct
    FROM ranked_promotions
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY promo_code, channel ORDER BY discount_pct, id) AS row_num
        FROM metrics
    ) t
    WHERE row_num = 1
),
metrics_promotions AS (
    -- Running and moving aggregates
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        latest_rank,
        latest_rank_2,
        next_discount_pct,
        discount_pct_bucket,
        discount_pct_rank,
        discount_pct_dense_rank,
        prev_discount_pct,
        row_num,
        COUNT(prev_discount_pct) OVER (PARTITION BY channel) channel_count_prev_discount_pct,
        SUM(discount_pct) OVER (PARTITION BY channel ORDER BY discount_pct, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_discount_pct,
        ROW_NUMBER() OVER (ORDER BY discount_pct DESC, id) seq_no
    FROM deduped
)
SELECT
    id,
    channel,
    promo_code,
    latest_rank_2,
    discount_pct_rank,
    discount_pct_dense_rank,
    prev_discount_pct,
    row_num,
    channel_count_prev_discount_pct
FROM metrics_promotions
