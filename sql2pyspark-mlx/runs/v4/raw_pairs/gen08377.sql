WITH valid AS (
    SELECT
        *,
        TRIM(channel) clean_channel,
        ROUND(discount_pct * 1.1, 2) discount_pct_scaled
    FROM promotions
    WHERE ends_on >= DATE '2024-02-12'
      AND ends_on < '2025-07-23'
),
valid_promotions AS (
    SELECT
        *,
        LENGTH(promo_code) promo_code_length,
        DATE_FORMAT(ends_on, 'MMM yyyy') ends_on_label,
        COALESCE(channel, 'OTHER') resolved_channel
    FROM valid
    WHERE ends_on >= DATE '2023-12-31'
      AND MONTH(starts_on) IN (8, 11, 12)
),
scored AS (
    SELECT
        *,
        FIRST_VALUE(discount_pct_scaled, TRUE) OVER (ORDER BY ends_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_discount_pct_scaled,
        SUM(promo_code_length) OVER (PARTITION BY promo_code ORDER BY promo_code_length, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_promo_code_length,
        AVG(discount_pct_scaled) OVER (ORDER BY promo_code_length, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) discount_pct_scaled_avg_2
    FROM valid_promotions
),
metrics AS (
    SELECT
        *,
        STDDEV_SAMP(discount_pct_scaled) OVER (PARTITION BY promo_code) promo_code_stddev_samp_discount_pct_scaled,
        FIRST_VALUE(promo_code_length) IGNORE NULLS OVER (ORDER BY promo_code_length, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_promo_code_length
    FROM scored
),
enriched AS (
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        clean_channel,
        discount_pct_scaled,
        promo_code_length,
        ends_on_label,
        resolved_channel,
        first_known_discount_pct_scaled,
        running_promo_code_length,
        discount_pct_scaled_avg_2,
        promo_code_stddev_samp_discount_pct_scaled,
        first_known_promo_code_length,
        NTILE(3) OVER (PARTITION BY channel ORDER BY discount_pct_scaled_avg_2, id) discount_pct_scaled_avg_2_bucket,
        ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY first_known_promo_code_length DESC, id) seq_no
    FROM metrics
)
SELECT
    CASE WHEN discount_pct_scaled_avg_2_bucket >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END AS discount_pct_scaled_avg_2_bucket_tier,
    COUNT_IF(promo_code = 'VIP') AS vip_events,
    SUM(CASE WHEN channel = 'partner' THEN discount_pct_scaled_avg_2 ELSE 0 END) AS partner_discount_pct_scaled_avg_2,
    COUNT(*) FILTER (WHERE promo_code = 'BUNDLE') AS bundle_rows
FROM enriched
GROUP BY 1
ORDER BY bundle_rows DESC NULLS FIRST;
