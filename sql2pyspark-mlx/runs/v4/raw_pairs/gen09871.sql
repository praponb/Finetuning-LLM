WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, DATE_ADD(starts_on, 1) AS starts_on_plus_1d
    FROM promotions
    WHERE ends_on >= DATE '2023-08-04'
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY ends_on DESC, id) AS latest_rank
    FROM cleaned
    QUALIFY latest_rank = 1
),
cleaned_promotions AS (
    -- Keep recent, valid rows
    SELECT
        *,
        DATEDIFF(CURRENT_DATE(), ends_on) AS days_since_ends_on,
        COALESCE(channel, 'DIRECT') AS channel_filled
    FROM ranked
    WHERE starts_on_plus_1d > '2023-03-05'
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        channel,
        promo_code,
        days_since_ends_on
    FROM cleaned_promotions
) src
PIVOT (
    AVG(days_since_ends_on) AS amount,
    COUNT(1) AS n
    FOR promo_code IN ('SPRING10' AS spring10, 'BUNDLE' AS bundle, 'WELCOME5' AS welcome5)
);
