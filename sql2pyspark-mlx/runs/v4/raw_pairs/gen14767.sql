WITH cleaned AS (
    SELECT *, LOWER(channel) AS channel_lower
    FROM promotions
    WHERE ends_on > TO_DATE('2023-12-16')
      AND channel IN ('web', 'partner')
),
attributed AS (
    SELECT
        src.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM cleaned src
    LEFT JOIN promotions pr
        ON src.channel = pr.channel
       AND src.ends_on >= pr.starts_on
       AND src.ends_on < pr.ends_on + INTERVAL 7 DAYS
),
scored AS (
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        channel_lower,
        promo_id,
        promotion_promo_code,
        promotion_discount_pct,
        SUM(promotion_discount_pct) OVER w_ordered AS cumulative_promotion_discount_pct,
        ROW_NUMBER() OVER (PARTITION BY promotion_promo_code ORDER BY discount_pct, id, promo_id) AS row_seq
    FROM attributed
    WINDOW w_ordered AS (PARTITION BY channel, promotion_promo_code ORDER BY starts_on, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
scored_promotions AS (
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        channel_lower,
        promo_id,
        promotion_promo_code,
        promotion_discount_pct,
        cumulative_promotion_discount_pct,
        row_seq,
        LAG(promo_id, 2) OVER (PARTITION BY promotion_promo_code ORDER BY cumulative_promotion_discount_pct, id, promo_id) AS prev_promo_id,
        LEAD(cumulative_promotion_discount_pct, 1) OVER (PARTITION BY channel ORDER BY cumulative_promotion_discount_pct, id, promo_id) AS next_cumulative_promotion_discount_pct,
        AVG(discount_pct) OVER (PARTITION BY promotion_promo_code ORDER BY row_seq, id, promo_id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS discount_pct_avg_5_prior,
        PERCENT_RANK() OVER (PARTITION BY channel, promotion_promo_code ORDER BY promotion_discount_pct DESC NULLS FIRST) AS promotion_discount_pct_percent_rank,
        AVG(promo_id) OVER (PARTITION BY channel ORDER BY discount_pct, id, promo_id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS promo_id_avg_5
    FROM scored
)
SELECT
    id,
    channel,
    promo_code,
    starts_on,
    ends_on,
    channel_lower,
    promo_id,
    promotion_promo_code,
    promotion_discount_pct
FROM scored_promotions;
