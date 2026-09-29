WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        discount_pct,
        DAY(starts_on) AS starts_on_day,
        COALESCE(channel, 'DIRECT') AS resolved_channel
    FROM promotions
    WHERE channel IS NOT NULL
      AND channel IN ('store', 'web')
),
matched_keys AS (
    -- Set intersection on keys
    SELECT id, channel
    FROM cleaned
    INTERSECT ALL
    SELECT id, channel
    FROM promotions
    WHERE channel IN ('store', 'mobile', 'web')
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM cleaned s
    JOIN matched_keys k ON s.id = k.id AND s.channel = k.channel
),
attributed AS (
    -- Non-equi join on a validity window
    SELECT
        e.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM verified e
    JOIN promotions pr
        ON e.channel = pr.channel
       AND e.starts_on >= pr.starts_on
       AND e.starts_on <= DATE_ADD(pr.ends_on, 10)
),
matched_keys_2 AS (
    -- Set intersection on keys
    SELECT promo_code
    FROM attributed
    INTERSECT
    SELECT promo_code
    FROM promotions
    WHERE promo_code NOT IN ('WELCOME5', 'SPRING10')
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM attributed s
    JOIN matched_keys_2 k ON s.promo_code = k.promo_code
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('QUARTER', starts_on) AS quarter_start,
    DATE_TRUNC('MONTH', starts_on) AS activity_month,
    PERCENTILE_APPROX(starts_on_day, ARRAY(0.25, 0.5, 0.75)) AS starts_on_day_quartiles,
    APPROX_PERCENTILE(starts_on_day, 0.5) AS starts_on_day_p50,
    SUM(CASE WHEN promotion_promo_code = 'WELCOME5' THEN promo_id ELSE 0 END) AS welcome5_promo_id,
    SUM(CASE WHEN promotion_promo_code = 'VIP' THEN promo_id ELSE 0 END) AS vip_promo_id,
    PERCENTILE_APPROX(discount_pct, ARRAY(0.25, 0.5, 0.75)) AS discount_pct_quartiles,
    DENSE_RANK() OVER (PARTITION BY DATE_TRUNC('QUARTER', starts_on) ORDER BY APPROX_PERCENTILE(starts_on_day, 0.5) DESC) AS starts_on_day_p50_dense_rank
FROM reconciled
GROUP BY quarter_start, activity_month
HAVING SUM(CASE WHEN promotion_promo_code = 'VIP' THEN promo_id ELSE 0 END) < 0
ORDER BY vip_promo_id;
