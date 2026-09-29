WITH cleaned AS (
    SELECT
        *,
        DATEDIFF(ends_on, starts_on) AS days_starts_on_to_ends_on,
        COALESCE(channel, 'UNKNOWN') AS channel_clean
    FROM promotions
    WHERE promo_code NOT IN ('SPRING10', 'VIP')
)
SELECT *
FROM (
    SELECT
        promo_code,
        channel,
        days_starts_on_to_ends_on
    FROM cleaned
) src
PIVOT (
    MAX(days_starts_on_to_ends_on) AS value_sum,
    COUNT(1) AS n
    FOR channel IN ('store', 'partner')
);
