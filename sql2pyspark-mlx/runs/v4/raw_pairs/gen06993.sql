WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CONCAT(channel, ' ', promo_code) AS channel_promo_code
    FROM promotions
    WHERE starts_on > DATE '2023-08-19'
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        DATEDIFF(CURRENT_DATE(), ends_on) AS days_since_ends_on,
        QUARTER(ends_on) AS ends_on_quarter
    FROM cleaned
    WHERE ends_on >= DATE '2024-03-06'
      AND channel IS NOT NULL
      AND discount_pct <= 0.27
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROUND(discount_pct / SUM(discount_pct) OVER (PARTITION BY channel), 4) AS discount_pct_share,
        MIN(days_since_ends_on) OVER (PARTITION BY promo_code) AS promo_code_min_days_since_ends_on
    FROM base
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        a.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM metrics a
    LEFT JOIN promotions pr
        ON COALESCE(a.channel, 'web') = pr.channel
       AND a.starts_on >= pr.starts_on
       AND a.starts_on < pr.ends_on + INTERVAL '30' DAY
)
-- Running and moving aggregates
SELECT
    *,
    NTILE(4) OVER (ORDER BY promo_id, id, promo_id) AS promo_id_bucket,
    FIRST_VALUE(promo_id) IGNORE NULLS OVER (ORDER BY ends_on_quarter, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_promo_id
FROM matched;
