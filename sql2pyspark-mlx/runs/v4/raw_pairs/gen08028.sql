WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        promo_code,
        starts_on,
        discount_pct,
        FLOOR(discount_pct) AS discount_pct_whole
    FROM promotions
    WHERE channel IS NOT NULL
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAST_VALUE(discount_pct_whole, TRUE) OVER (ORDER BY discount_pct_whole, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_discount_pct_whole,
        AVG(discount_pct) OVER (PARTITION BY promo_code ORDER BY discount_pct_whole, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS discount_pct_avg_6
    FROM valid
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CASE WHEN discount_pct >= 0.33 THEN 'High' WHEN discount_pct >= 0.14 THEN 'Medium' ELSE 'Low' END AS discount_pct_band
    FROM metrics
    WHERE starts_on > DATE '2024-01-13'
),
valid_promotions AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        starts_on,
        discount_pct,
        discount_pct_whole,
        last_known_discount_pct_whole,
        discount_pct_band,
        WEEKOFYEAR(starts_on) AS starts_on_weekofyear
    FROM base
    WHERE starts_on > DATE '2023-10-04'
),
summary AS (
    -- Grouped metrics
    SELECT
        CASE WHEN discount_pct_whole >= 64 THEN 'LARGE' ELSE 'SMALL' END AS discount_pct_whole_tier,
        CASE WHEN discount_pct >= 0.3 THEN 'PREMIUM' ELSE 'STANDARD' END AS discount_pct_tier,
        COUNT(*) AS num_rows,
        PERCENTILE_APPROX(last_known_discount_pct_whole, 0.95) AS last_known_discount_pct_whole_p95,
        COUNT(*) AS event_count
    FROM valid_promotions
    GROUP BY discount_pct_whole_tier, discount_pct_tier
)
SELECT
    discount_pct_whole_tier,
    discount_pct_tier,
    num_rows,
    last_known_discount_pct_whole_p95,
    event_count
FROM summary
ORDER BY last_known_discount_pct_whole_p95 NULLS LAST;
