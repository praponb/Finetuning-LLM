WITH cleaned AS (
    SELECT
        *,
        INITCAP(TRIM(promo_code)) AS promo_code_title,
        COALESCE(channel, 'DIRECT') AS resolved_channel
    FROM promotions
    WHERE starts_on >= DATE '2023-05-27'
      AND ends_on <= '2025-10-01'
)
SELECT *
FROM (
    SELECT
        promo_code,
        channel,
        discount_pct
    FROM cleaned
) src
PIVOT (
    SUM(discount_pct) AS total,
    COUNT(1) AS cnt
    FOR channel IN ('web' AS web, 'store' AS store)
);
