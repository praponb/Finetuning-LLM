WITH base AS (
    SELECT *, DATE_SUB(starts_on, 9) AS starts_on_minus_9d
    FROM promotions
    WHERE ends_on >= DATE '2023-07-29'
      AND ends_on < '2023-12-15'
),
attributed AS (
    SELECT
        s.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM base s
    LEFT JOIN promotions pr
        ON s.channel <=> pr.channel
       AND s.starts_on >= pr.starts_on
       AND s.starts_on < pr.ends_on + INTERVAL '30' DAY
),
matched_keys AS (
    SELECT id
    FROM attributed
    INTERSECT
    SELECT id
    FROM promotions
    WHERE ends_on < '2025-02-01'
),
kept_rows AS (
    SELECT s.*
    FROM attributed s
    JOIN matched_keys k ON s.id = k.id
),
kept_keys AS (
    SELECT id, promo_code
    FROM kept_rows
    EXCEPT
    SELECT id, promo_code
    FROM promotions
    WHERE ends_on <= '2023-08-06'
),
verified AS (
    SELECT s.*
    FROM kept_rows s
    JOIN kept_keys k ON s.id = k.id AND s.promo_code = k.promo_code
),
matched AS (
    SELECT
        a.*,
        pr.id AS promo_id_2,
        pr.promo_code AS promotion_promo_code_2,
        pr.discount_pct AS promotion_discount_pct_2
    FROM verified a
    JOIN promotions pr
        ON a.channel <=> pr.channel
       AND a.starts_on_minus_9d >= pr.starts_on
       AND a.starts_on_minus_9d < pr.ends_on + INTERVAL '14' DAY
),
with_window AS (
    SELECT
        src.*,
        pr.id AS promo_id_3,
        pr.promo_code AS promotion_promo_code_3,
        pr.discount_pct AS promotion_discount_pct_3
    FROM matched src
    JOIN promotions pr
        ON COALESCE(src.channel, 'web') = pr.channel
       AND src.ends_on >= pr.starts_on
       AND src.ends_on < pr.ends_on + INTERVAL '14' DAY
),
base_promotions AS (
    SELECT *, ROUND(promotion_discount_pct_3 * 0.85, 2) AS adjusted_promotion_discount_pct_3
    FROM with_window
    WHERE promotion_promo_code IN ('WELCOME5', 'VIP')
      AND channel IS NOT NULL
),
totals AS (
    SELECT
        DATE_TRUNC('WEEK', starts_on_minus_9d) AS week_start,
        channel,
        DATE_TRUNC('QUARTER', starts_on) AS quarter_start,
        ROUND(AVG(promotion_discount_pct), 2) AS avg_promotion_discount_pct,
        COUNT(CASE WHEN promotion_promo_code_3 = 'SPRING10' THEN 1 END) AS spring10_count,
        PERCENT_RANK() OVER (PARTITION BY DATE_TRUNC('WEEK', starts_on_minus_9d) ORDER BY ROUND(AVG(promotion_discount_pct), 2) DESC) AS avg_promotion_discount_pct_percent_rank
    FROM base_promotions
    GROUP BY DATE_TRUNC('WEEK', starts_on_minus_9d), channel, DATE_TRUNC('QUARTER', starts_on)
    HAVING spring10_count >= 1
)
SELECT
    week_start,
    channel,
    quarter_start,
    avg_promotion_discount_pct,
    spring10_count
FROM totals
ORDER BY spring10_count ASC NULLS LAST;
