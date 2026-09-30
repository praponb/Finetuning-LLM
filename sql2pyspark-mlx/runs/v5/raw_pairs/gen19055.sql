WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        currency,
        element
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_limits) x AS element
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        effective_from,
        currency,
        element,
        TRIM(currency) clean_currency
    FROM lines
    WHERE effective_from >= TO_DATE('2024-04-17')
      AND currency = 'USD'
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        effective_from,
        currency,
        element,
        DAY(effective_from) effective_from_day
    FROM filtered
    WHERE effective_from > DATE '2024-06-20'
      AND currency = 'USD'
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        currency,
        element,
        effective_from_day,
        MIN(element) OVER w_ordered currency_min_element,
        MAX(effective_from_day) OVER w_ordered currency_max_effective_from_day
    FROM base
    WINDOW w_ordered AS (PARTITION BY currency)
),
metrics AS (
    -- Ranking and gap features
    SELECT *, DENSE_RANK() OVER (PARTITION BY currency ORDER BY currency_min_element DESC NULLS LAST) currency_min_element_dense_rank
    FROM enriched
    WHERE effective_from_day > 65
)
SELECT
    currency,
    element,
    currency_min_element,
    currency_max_effective_from_day
FROM metrics
