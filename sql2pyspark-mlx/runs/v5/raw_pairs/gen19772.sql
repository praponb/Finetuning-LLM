WITH matched AS (
    SELECT
        src.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM promotions src
    INNER JOIN promotions pr
        ON src.channel = pr.channel
       AND src.starts_on BETWEEN pr.starts_on AND pr.ends_on
),
with_window AS (
    SELECT
        a.*,
        pr.id AS promo_id_2,
        pr.promo_code AS promotion_promo_code_2,
        pr.discount_pct AS promotion_discount_pct_2
    FROM matched a
    LEFT JOIN promotions pr
        ON a.channel <=> pr.channel
       AND a.starts_on >= pr.starts_on
       AND a.starts_on <= DATE_ADD(pr.ends_on, 10)
),
scored AS (
    SELECT
        *,
        SUM(promo_id_2) OVER w_running AS cumulative_promo_id_2,
        SUM(promotion_discount_pct) OVER (PARTITION BY promotion_promo_code_2, promo_code ORDER BY starts_on, id, promo_id, promo_id_2 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_promotion_discount_pct,
        MIN(discount_pct) OVER (PARTITION BY promo_code) AS promo_code_min_discount_pct
    FROM with_window
    WHERE promo_id_2 <> 9
    WINDOW w_running AS (ORDER BY promotion_discount_pct_2, id, promo_id, promo_id_2 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY cumulative_promo_id_2 DESC, id, promo_id, promo_id_2) AS latest_rank
    FROM scored
    QUALIFY latest_rank = 1
)
SELECT
    channel,
    DATE_TRUNC('MONTH', ends_on) AS activity_month,
    ROUND(AVG(promo_id), 2) AS avg_promo_id,
    ROUND(SUM(running_promotion_discount_pct) / NULLIF(COUNT(CASE WHEN channel = 'mobile' THEN 1 END), 0), 2) AS running_promotion_discount_pct_per_mobile,
    SUM(CASE WHEN channel = 'partner' THEN running_promotion_discount_pct ELSE 0 END) AS partner_running_promotion_discount_pct,
    COUNT(DISTINCT channel) AS distinct_channels
FROM ranked
GROUP BY 1, 2
HAVING ROUND(SUM(running_promotion_discount_pct) / NULLIF(COUNT(CASE WHEN channel = 'mobile' THEN 1 END), 0), 2) >= 0;
