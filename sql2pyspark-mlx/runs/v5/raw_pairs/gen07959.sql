SELECT *
FROM (
    SELECT
        channel,
        promo_code,
        discount_pct
    FROM promotions
) src
PIVOT (
    MAX(discount_pct)
    FOR promo_code IN ('SPRING10', 'VIP', 'WELCOME5')
);
