WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        channel,
        starts_on,
        ends_on,
        WEEKOFYEAR(ends_on) AS ends_on_weekofyear,
        CASE WHEN channel IN ('mobile', 'store') THEN 'Group A' ELSE 'Other' END AS channel_group,
        COALESCE(channel, 'OTHER') AS channel_filled
    FROM promotions
    WHERE ends_on >= DATE '2024-06-05'
      AND promo_code != 'BUNDLE'
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        s.*,
        pr.id AS promo_id,
        pr.promo_code,
        pr.discount_pct
    FROM cleaned s
    INNER JOIN promotions pr
        ON COALESCE(s.channel, 'store') = pr.channel
       AND s.starts_on BETWEEN pr.starts_on AND pr.ends_on
),
matched_promotions AS (
    -- Non-equi join on a validity window
    SELECT
        s.*,
        pr.id AS promo_id_2,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM matched s
    LEFT JOIN promotions pr
        ON s.channel <=> pr.channel
       AND s.starts_on >= pr.starts_on
       AND s.starts_on <= DATE_ADD(pr.ends_on, 7)
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, promo_id * discount_pct AS promo_id_times_discount_pct
    FROM matched_promotions
    WHERE promotion_promo_code IS NOT NULL
),
wide AS (
    -- Native SQL PIVOT
    SELECT *
    FROM (
        SELECT
            channel,
            promo_code,
            promo_id
        FROM base
    ) src
    PIVOT (
        MAX(promo_id)
        FOR promo_code IN ('BUNDLE', 'SPRING10', 'WELCOME5')
    )
)
SELECT
    channel,
    BUNDLE,
    SPRING10,
    WELCOME5
FROM wide
ORDER BY BUNDLE DESC
