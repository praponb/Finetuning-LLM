WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        channel,
        promo_code,
        ends_on,
        discount_pct,
        TRIM(promo_code) clean_promo_code,
        COALESCE(channel, 'N/A') channel_filled
    FROM promotions
    WHERE ends_on >= TO_DATE('2023-10-02')
      AND channel IS NOT NULL
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM cleaned
    QUALIFY RANK() OVER (PARTITION BY promo_code, channel ORDER BY ends_on) = 1
),
attributed AS (
    -- Non-equi join on a validity window
    SELECT
        a.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM ranked a
    LEFT JOIN promotions pr
        ON COALESCE(a.channel, 'web') = pr.channel
       AND a.ends_on >= pr.starts_on
       AND a.ends_on < pr.ends_on + INTERVAL '14' DAY
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        src.*,
        pr.id AS promo_id_2,
        pr.promo_code AS promotion_promo_code_2,
        pr.discount_pct AS promotion_discount_pct_2
    FROM attributed src
    LEFT JOIN promotions pr
        ON COALESCE(src.channel, 'web') = pr.channel
       AND src.ends_on >= pr.starts_on
       AND src.ends_on <= DATE_ADD(pr.ends_on, 10)
),
matched_promotions AS (
    -- Non-equi join on a validity window
    SELECT
        e.*,
        pr.id AS promo_id_3,
        pr.promo_code AS promotion_promo_code_3,
        pr.discount_pct AS promotion_discount_pct_3
    FROM matched e
    LEFT JOIN promotions pr
        ON e.channel = pr.channel
       AND e.ends_on BETWEEN pr.starts_on AND pr.ends_on
)
-- Ranking and gap features
SELECT
    *,
    PERCENT_RANK() OVER (PARTITION BY promotion_promo_code_3 ORDER BY promo_id_2 DESC) promo_id_2_percent_rank,
    LAG(promotion_discount_pct_3, 1, 0) OVER (ORDER BY promo_id, id, promo_id, promo_id_2, promo_id_3) prev_promotion_discount_pct_3,
    ROW_NUMBER() OVER (ORDER BY promo_id DESC, id, promo_id, promo_id_2, promo_id_3) seq_no,
    SUM(promotion_discount_pct_2) OVER (PARTITION BY promo_code) promo_code_sum_promotion_discount_pct_2
FROM matched_promotions;
