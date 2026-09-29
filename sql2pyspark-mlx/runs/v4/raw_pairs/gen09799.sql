WITH ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY discount_pct DESC, id) AS row_num
        FROM promotions
    ) x
    WHERE row_num = 1
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY channel, promo_code ORDER BY ends_on DESC, id) AS latest_rank
        FROM ranked
    ) t
    WHERE latest_rank = 1
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN promo_code IN ('SPRING10', 'VIP') THEN 'Core' ELSE 'Other' END AS promo_code_group,
        COALESCE(channel, 'OTHER') AS resolved_channel
    FROM latest
    WHERE channel IS NOT NULL
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        a.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM cleaned a
    LEFT JOIN promotions pr
        ON COALESCE(a.channel, 'store') = pr.channel
       AND a.starts_on BETWEEN pr.starts_on AND pr.ends_on
),
with_window_promotions AS (
    -- Non-equi join on a validity window
    SELECT
        a.*,
        pr.id AS promo_id_2,
        pr.promo_code AS promotion_promo_code_2,
        pr.discount_pct AS promotion_discount_pct_2
    FROM with_window a
    LEFT JOIN promotions pr
        ON COALESCE(a.channel, 'web') = pr.channel
       AND a.starts_on BETWEEN pr.starts_on AND pr.ends_on
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        promotion_promo_code,
        channel,
        promo_code,
        promo_id_2
    FROM with_window_promotions
) src
PIVOT (
    AVG(promo_id_2) AS total,
    COUNT(1) AS n
    FOR promo_code IN ('BUNDLE', 'SPRING10', 'WELCOME5')
)
