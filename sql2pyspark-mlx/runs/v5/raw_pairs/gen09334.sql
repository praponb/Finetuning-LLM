WITH exploded AS (
    SELECT
        id,
        effective_from,
        currency,
        element
    FROM price_tiers
    LATERAL VIEW EXPLODE(discounts) x AS element
),
base AS (
    SELECT
        *,
        INITCAP(TRIM(currency)) AS currency_title,
        DATE_ADD(effective_from, 20) AS effective_from_plus_20d
    FROM exploded
    WHERE effective_from > DATE '2024-02-08'
),
enriched AS (
    SELECT
        effective_from,
        currency,
        element,
        currency_title,
        effective_from_plus_20d,
        CUME_DIST() OVER (ORDER BY element DESC NULLS LAST) AS element_cume_dist,
        STDDEV_SAMP(element) OVER (PARTITION BY currency) AS currency_stddev_samp_element,
        PERCENT_RANK() OVER (PARTITION BY currency ORDER BY element) AS element_percent_rank,
        COUNT(element) OVER (PARTITION BY currency) AS currency_count_element,
        DENSE_RANK() OVER (PARTITION BY currency ORDER BY element DESC NULLS FIRST) AS element_dense_rank
    FROM base
),
matched_keys AS (
    SELECT currency
    FROM enriched
    INTERSECT ALL
    SELECT currency
    FROM price_tiers
    WHERE plan IN ('enterprise', 'starter')
),
verified AS (
    SELECT s.*
    FROM enriched s
    INNER JOIN matched_keys k ON s.currency = k.currency
),
scored AS (
    SELECT
        effective_from,
        currency,
        currency_title,
        effective_from_plus_20d,
        element_cume_dist,
        element_percent_rank,
        currency_count_element,
        RANK() OVER (ORDER BY element_cume_dist NULLS LAST) AS element_cume_dist_rank
    FROM verified
),
cleaned AS (
    SELECT *, element_percent_rank + currency_count_element AS element_percent_rank_plus_currency_count_element
    FROM scored
    WHERE effective_from > DATE '2024-05-23'
)
SELECT
    effective_from,
    currency,
    currency_title,
    effective_from_plus_20d,
    element_cume_dist,
    element_percent_rank,
    currency_count_element
FROM cleaned;
