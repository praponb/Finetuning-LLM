WITH valid AS (
    SELECT
        id,
        channel,
        promo_code,
        ends_on,
        discount_pct,
        DATEDIFF(CURRENT_DATE(), starts_on) AS days_since_starts_on,
        COALESCE(channel, 'UNKNOWN') AS channel_clean
    FROM promotions
    WHERE starts_on >= TO_DATE('2023-12-20')
      AND channel IS NOT NULL
      AND ends_on <= '2025-03-01'
)
SELECT *
FROM (
    SELECT
        promo_code,
        channel,
        discount_pct
    FROM valid
) src
PIVOT (
    SUM(discount_pct) AS value_sum,
    COUNT(1) AS cnt
    FOR channel IN ('store', 'web')
);
