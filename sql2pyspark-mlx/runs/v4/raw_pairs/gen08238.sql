WITH filtered AS (
    SELECT
        *,
        FLOOR(discount_pct) AS discount_pct_whole,
        COALESCE(channel, 'OTHER') AS resolved_channel
    FROM promotions
    WHERE ends_on >= DATE '2024-02-04'
),
filtered_promotions AS (
    SELECT
        id,
        starts_on,
        discount_pct,
        discount_pct_whole,
        resolved_channel,
        LEAST(discount_pct_whole, discount_pct) AS least_discount_pct_whole_discount_pct
    FROM filtered
    WHERE ends_on > DATE '2023-08-27'
      AND channel IS NOT NULL
),
cleaned AS (
    SELECT
        id,
        discount_pct,
        discount_pct_whole,
        resolved_channel,
        least_discount_pct_whole_discount_pct,
        CASE WHEN discount_pct_whole >= 82 THEN 'High' WHEN discount_pct_whole >= 33 THEN 'Medium' ELSE 'Low' END AS discount_pct_whole_tier
    FROM filtered_promotions
    WHERE discount_pct > 0.21
)
SELECT
    id,
    discount_pct,
    resolved_channel,
    discount_pct_whole_tier
FROM cleaned
