WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CEIL(discount_pct) AS discount_pct_whole,
        SUBSTRING(promo_code, 1, 4) AS promo_code_prefix,
        COALESCE(channel, 'DIRECT') AS resolved_channel
    FROM promotions
    WHERE discount_pct < 0.24
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        discount_pct_whole,
        promo_code_prefix,
        resolved_channel,
        UPPER(channel) AS channel_upper
    FROM cleaned
    WHERE starts_on >= DATE '2023-08-21'
      AND YEAR(ends_on) >= 2023
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        a.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM valid a
    INNER JOIN promotions pr
        ON COALESCE(a.channel, 'web') = pr.channel
       AND a.ends_on >= pr.starts_on
       AND a.ends_on <= DATE_ADD(pr.ends_on, 7)
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        starts_on,
        ends_on,
        discount_pct_whole,
        promo_code_prefix,
        channel_upper,
        promo_id,
        promotion_promo_code
    FROM (
        SELECT *, RANK() OVER (PARTITION BY promo_code ORDER BY starts_on DESC) AS row_num
        FROM with_window
    ) x
    WHERE row_num = 1
)
-- Grouped metrics
SELECT
    CASE WHEN promo_id >= 9 THEN 'HIGH' ELSE 'LOW' END AS promo_id_tier,
    DATE_TRUNC('WEEK', ends_on) AS week_start,
    SORT_ARRAY(COLLECT_SET(promotion_promo_code)) AS promotion_promo_code_list,
    AVG(discount_pct_whole) AS avg_discount_pct_whole
FROM latest
GROUP BY CASE WHEN promo_id >= 9 THEN 'HIGH' ELSE 'LOW' END, DATE_TRUNC('WEEK', ends_on)
ORDER BY week_start
