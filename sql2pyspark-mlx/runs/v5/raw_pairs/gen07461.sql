WITH base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        TRIM(channel) AS clean_channel,
        CASE WHEN promo_code = 'SPRING10' THEN 1 ELSE 0 END AS is_spring10,
        COALESCE(channel, 'DIRECT') AS channel_clean
    FROM promotions
    WHERE starts_on > DATE '2023-07-13'
      AND channel IS NOT NULL
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        e.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM base e
    JOIN promotions pr
        ON e.channel <=> pr.channel
       AND e.ends_on >= pr.starts_on
       AND e.ends_on <= DATE_ADD(pr.ends_on, 10)
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM matched
    QUALIFY RANK() OVER (PARTITION BY promo_code, promotion_promo_code ORDER BY is_spring10 DESC NULLS LAST) = 1
),
kept_keys AS (
    -- Set difference on keys
    SELECT id
    FROM ranked
    EXCEPT
    SELECT id
    FROM promotions
    WHERE channel IN ('mobile', 'partner', 'web')
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM ranked s
    JOIN kept_keys k ON s.id = k.id
),
attributed AS (
    -- Non-equi join on a validity window
    SELECT
        e.*,
        pr.id AS promo_id_2,
        pr.promo_code AS promotion_promo_code_2,
        pr.discount_pct AS promotion_discount_pct_2
    FROM reconciled e
    LEFT JOIN promotions pr
        ON e.channel = pr.channel
       AND e.ends_on >= pr.starts_on
       AND e.ends_on <= DATE_ADD(pr.ends_on, 7)
),
matched_promotions AS (
    -- Non-equi join on a validity window
    SELECT
        s.*,
        pr.id AS promo_id_3,
        pr.promo_code AS promotion_promo_code_3,
        pr.discount_pct AS promotion_discount_pct_3
    FROM attributed s
    LEFT JOIN promotions pr
        ON s.channel <=> pr.channel
       AND s.ends_on >= pr.starts_on
       AND s.ends_on <= DATE_ADD(pr.ends_on, 7)
)
-- Aggregate with conditional metrics
SELECT
    channel,
    promotion_promo_code_3,
    COUNT(*) FILTER (WHERE promotion_promo_code = 'VIP') AS vip_rows,
    SUM(CASE WHEN promotion_promo_code_2 = 'WELCOME5' THEN promo_id_3 ELSE 0 END) AS welcome5_promo_id_3,
    COUNT(*) AS row_count
FROM matched_promotions
GROUP BY channel, promotion_promo_code_3
HAVING SUM(CASE WHEN promotion_promo_code_2 = 'WELCOME5' THEN promo_id_3 ELSE 0 END) >= 0
ORDER BY vip_rows DESC
