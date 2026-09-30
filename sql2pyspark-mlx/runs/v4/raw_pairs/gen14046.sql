WITH cleaned AS (
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        CASE WHEN channel = 'store' THEN 1 ELSE 0 END is_store,
        COALESCE(channel, 'DIRECT') resolved_channel
    FROM promotions
    WHERE ends_on >= DATE '2023-11-10'
      AND ends_on < '2023-09-23'
),
attributed AS (
    SELECT
        a.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct
    FROM cleaned a
    LEFT JOIN promotions pr
        ON COALESCE(a.channel, 'web') = pr.channel
       AND a.starts_on >= pr.starts_on
       AND a.starts_on <= DATE_ADD(pr.ends_on, 3)
),
cleaned_promotions AS (
    SELECT
        *,
        CONCAT(promotion_promo_code, '/', channel) promotion_promo_code_channel,
        LENGTH(promotion_promo_code) promotion_promo_code_length
    FROM attributed
    WHERE starts_on > DATE '2023-09-18'
      AND channel IS NOT NULL
)
SELECT
    DATE_TRUNC('QUARTER', starts_on) AS quarter_start,
    STDDEV_SAMP(discount_pct) AS discount_pct_stddev,
    APPROX_PERCENTILE(is_store, 0.5) AS is_store_p50,
    SUM(is_store) AS total_is_store
FROM cleaned_promotions
GROUP BY quarter_start
ORDER BY discount_pct_stddev ASC
