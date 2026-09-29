WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN promo_code = 'WELCOME5' THEN 1 ELSE 0 END AS is_welcome5,
        COALESCE(channel, 'N/A') AS channel_filled
    FROM promotions
    WHERE ends_on > DATE '2023-07-31'
      AND channel IS NOT DISTINCT FROM 'store'
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM cleaned
    QUALIFY ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY is_welcome5, id) = 1
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
        is_welcome5,
        channel_filled,
        LAST_DAY(ends_on) AS ends_on_month_end,
        ROUND(is_welcome5, 2) AS is_welcome5_rounded
    FROM deduped
    WHERE starts_on >= TO_DATE('2023-07-31')
      AND discount_pct BETWEEN 0.2 AND 0.26
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY is_welcome5_rounded, id) AS seq_no,
        ROW_NUMBER() OVER (PARTITION BY channel ORDER BY ends_on_month_end DESC, id) AS event_seq,
        FIRST_VALUE(is_welcome5, TRUE) OVER w_part AS first_known_is_welcome5,
        LAST_VALUE(is_welcome5, TRUE) OVER (ORDER BY ends_on_month_end, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_is_welcome5
    FROM valid
    WINDOW w_part AS (PARTITION BY promo_code ORDER BY starts_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
attributed AS (
    -- Non-equi join on a validity window
    SELECT
        a.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM with_windows a
    LEFT JOIN promotions pr
        ON COALESCE(a.channel, 'web') = pr.channel
       AND a.starts_on >= pr.starts_on
       AND a.starts_on <= DATE_ADD(pr.ends_on, 3)
)
-- Native SQL UNPIVOT (wide to long)
SELECT
    id,
    promo_id,
    promotion_promo_code,
    metric,
    value
FROM (
    SELECT
        id,
        promo_id,
        promotion_promo_code,
        promotion_discount_pct,
        discount_pct
    FROM attributed
) src
UNPIVOT (
    value FOR metric IN (promotion_discount_pct, discount_pct)
);
