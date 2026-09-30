WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        DATE_FORMAT(ends_on, 'yyyy-MM') AS ends_on_label,
        COALESCE(channel, 'DIRECT') AS channel_clean
    FROM promotions
    WHERE ends_on >= DATE '2023-09-16'
      AND promo_code = 'BUNDLE'
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        promo_code,
        ends_on,
        discount_pct,
        ends_on_label,
        channel_clean
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY starts_on DESC, id) AS dedup_rank
        FROM valid
    ) t
    WHERE dedup_rank = 1
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(discount_pct) OVER (PARTITION BY promo_code) AS promo_code_sum_discount_pct,
        NTILE(5) OVER (ORDER BY discount_pct DESC, id) AS discount_pct_bucket
    FROM ranked
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, COALESCE(discount_pct_bucket, 0) AS discount_pct_bucket_filled
    FROM metrics
    WHERE ends_on >= DATE '2023-07-03'
      AND ends_on < '2024-02-27'
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('MONTH', ends_on) AS activity_month,
        CASE WHEN discount_pct_bucket_filled >= 89 THEN 'HIGH' ELSE 'LOW' END AS discount_pct_bucket_filled_tier,
        APPROX_PERCENTILE(discount_pct_bucket_filled, ARRAY(0.25, 0.5, 0.75)) AS discount_pct_bucket_filled_quartiles,
        ROUND(AVG(discount_pct), 2) AS avg_discount_pct,
        ROUND(AVG(discount_pct), 2) AS avg_discount_pct_2,
        MAX(promo_code_sum_discount_pct) FILTER (WHERE promo_code = 'BUNDLE') AS bundle_max_promo_code_sum_discount_pct
    FROM base
    GROUP BY 1, 2
    HAVING avg_discount_pct_2 >= 0
)
SELECT
    activity_month,
    discount_pct_bucket_filled_tier,
    avg_discount_pct
FROM grouped;
