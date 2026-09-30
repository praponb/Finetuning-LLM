WITH exploded AS (
    SELECT
        id,
        effective_from,
        currency,
        EXPLODE(discounts) AS element
    FROM price_tiers
),
scored AS (
    SELECT *, CUME_DIST() OVER (PARTITION BY currency ORDER BY element DESC) AS element_cume_dist
    FROM exploded
),
cleaned AS (
    SELECT *, ROUND(element * 1.1, 2) AS element_scaled
    FROM scored
    WHERE effective_from > TO_DATE('2024-05-05')
      AND currency = 'EUR'
),
cleaned_price_tiers AS (
    SELECT *, CAST(element AS INT) AS element_int
    FROM cleaned
    WHERE effective_from >= DATE '2024-06-20'
),
scored_price_tiers AS (
    SELECT
        *,
        SUM(element_cume_dist) OVER (PARTITION BY currency) AS currency_sum_element_cume_dist,
        MAX(element_int) OVER (PARTITION BY currency) AS currency_max_element_int,
        CUME_DIST() OVER (ORDER BY element_int DESC NULLS LAST) AS element_int_cume_dist,
        AVG(element) OVER (PARTITION BY currency) AS currency_avg_element
    FROM cleaned_price_tiers
)
SELECT
    currency,
    COUNT(DISTINCT id) AS distinct_ids,
    ROUND(AVG(currency_avg_element), 2) AS avg_currency_avg_element
FROM scored_price_tiers
GROUP BY currency
HAVING avg_currency_avg_element >= 0;
