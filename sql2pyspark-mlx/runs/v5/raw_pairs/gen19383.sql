WITH valid AS (
    SELECT *, DAYOFWEEK(starts_on) AS starts_on_dayofweek
    FROM promotions
    WHERE ends_on >= DATE '2023-10-13'
      AND channel IS NOT NULL
)
SELECT *
FROM (
    SELECT
        channel,
        promo_code,
        discount_pct
    FROM valid
) src
PIVOT (
    AVG(discount_pct) AS total
    FOR promo_code IN ('WELCOME5', 'SPRING10', 'BUNDLE')
);
