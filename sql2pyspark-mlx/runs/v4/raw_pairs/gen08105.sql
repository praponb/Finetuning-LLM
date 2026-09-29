WITH scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        PERCENT_RANK() OVER w_recent AS discount_pct_percent_rank,
        ROUND(discount_pct / SUM(discount_pct) OVER (PARTITION BY channel, promo_code), 4) AS discount_pct_share
    FROM promotions
    WINDOW w_recent AS (PARTITION BY promo_code ORDER BY discount_pct DESC)
),
scored_promotions AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY discount_pct_share DESC, id) AS row_seq,
        FIRST_VALUE(discount_pct_share, TRUE) OVER (ORDER BY discount_pct_percent_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_discount_pct_share,
        MAX(discount_pct) OVER (PARTITION BY promo_code) AS promo_code_max_discount_pct,
        NTILE(4) OVER (ORDER BY discount_pct DESC, id) AS discount_pct_bucket,
        LAST_VALUE(channel) IGNORE NULLS OVER (PARTITION BY promo_code ORDER BY discount_pct_percent_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_channel
    FROM scored
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        s.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM scored_promotions s
    INNER JOIN promotions pr
        ON s.channel = pr.channel
       AND s.ends_on >= pr.starts_on
       AND s.ends_on < pr.ends_on + INTERVAL 14 DAYS
)
SELECT
    id,
    starts_on,
    ends_on,
    discount_pct_percent_rank,
    discount_pct_share,
    row_seq,
    promo_code_max_discount_pct,
    discount_pct_bucket,
    promo_id
FROM with_window
