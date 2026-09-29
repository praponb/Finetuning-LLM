WITH exploded AS (
    SELECT
        id,
        effective_from,
        currency,
        element
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_prices) e AS element
),
scored AS (
    SELECT
        id,
        effective_from,
        currency,
        element,
        RANK() OVER w_grp AS element_rank,
        MIN(element) OVER w_recent AS currency_min_element,
        AVG(element) OVER w_recent AS currency_avg_element,
        PERCENT_RANK() OVER (PARTITION BY currency ORDER BY element DESC NULLS LAST) AS element_percent_rank
    FROM exploded
    WINDOW w_grp AS (ORDER BY element NULLS LAST), w_recent AS (PARTITION BY currency)
),
remaining_keys AS (
    SELECT id
    FROM scored
    EXCEPT ALL
    SELECT id
    FROM price_tiers
    WHERE YEAR(effective_from) = 2024
),
verified AS (
    SELECT s.*
    FROM scored s
    INNER JOIN remaining_keys k ON s.id = k.id
),
cleaned AS (
    SELECT *, ROUND(currency_avg_element * 1.1, 2) AS adjusted_currency_avg_element
    FROM verified
    WHERE effective_from > TO_DATE('2024-06-10')
      AND currency_min_element <= 40
),
enriched AS (
    SELECT
        id,
        effective_from,
        currency,
        element,
        element_rank,
        currency_min_element,
        currency_avg_element,
        element_percent_rank,
        adjusted_currency_avg_element,
        DENSE_RANK() OVER (PARTITION BY currency ORDER BY element_percent_rank DESC NULLS FIRST) AS element_percent_rank_dense_rank,
        MIN(adjusted_currency_avg_element) OVER w_running AS currency_min_adjusted_currency_avg_element,
        STDDEV_SAMP(currency_min_element) OVER w_running AS currency_stddev_samp_currency_min_element
    FROM cleaned
    WINDOW w_running AS (PARTITION BY currency)
),
remaining_keys_2 AS (
    SELECT id, currency
    FROM enriched
    EXCEPT DISTINCT
    SELECT id, currency
    FROM price_tiers
    WHERE plan IN ('enterprise', 'starter')
),
kept_rows AS (
    SELECT s.*
    FROM enriched s
    INNER JOIN remaining_keys_2 k ON s.id = k.id AND s.currency = k.currency
),
cleaned_price_tiers AS (
    SELECT *, COALESCE(currency_min_element, 0) AS currency_min_element_filled
    FROM kept_rows
    WHERE currency_min_element IS NOT NULL
)
SELECT
    id,
    effective_from,
    currency,
    element,
    currency_min_element,
    currency_avg_element,
    element_percent_rank_dense_rank,
    currency_min_adjusted_currency_avg_element,
    currency_stddev_samp_currency_min_element,
    CASE WHEN currency RLIKE '^(USD|THB)$' THEN 'PRIORITY' ELSE 'OTHER_ZONE' END AS currency_zone
FROM cleaned_price_tiers;
