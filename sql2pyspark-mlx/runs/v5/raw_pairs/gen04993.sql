WITH exploded AS (
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
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MIN(element) OVER (PARTITION BY currency) AS currency_min_element,
        RANK() OVER (PARTITION BY currency ORDER BY element NULLS LAST) AS element_rank
    FROM exploded
    WHERE plan IN ('starter', 'basic')
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        element_rank / NULLIF(element, 0) AS element_rank_per_element,
        SUBSTRING(plan, 1, 3) AS plan_prefix
    FROM scored
    WHERE element > 56
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, DATE_TRUNC('year', effective_from) AS effective_from_period
    FROM base
    WHERE element BETWEEN 59 AND 75
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        LOWER(currency) AS currency_lower,
        DATE_FORMAT(effective_from_period, 'MMM yyyy') AS effective_from_period_label
    FROM cleaned
    WHERE currency = 'EUR'
),
valid_price_tiers AS (
    -- Keep recent, valid rows
    SELECT *, element_rank_per_element + currency_min_element AS element_rank_per_element_plus_currency_min_element
    FROM valid
    WHERE effective_from BETWEEN '2024-04-11' AND '2025-06-02'
),
base_price_tiers AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CONCAT(plan, '/', currency) AS plan_currency
    FROM valid_price_tiers
    WHERE effective_from >= DATE '2024-05-11'
)
SELECT
    id,
    effective_from,
    element,
    plan_prefix,
    currency_lower,
    plan_currency
FROM base_price_tiers
ORDER BY effective_from DESC NULLS LAST
