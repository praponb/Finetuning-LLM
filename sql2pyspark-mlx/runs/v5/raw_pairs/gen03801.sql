WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        currency,
        EXPLODE(tier_prices) AS tier_value
    FROM price_tiers
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        plan,
        currency,
        CASE WHEN currency IN ('USD', 'THB') THEN 'Core' ELSE 'Other' END AS currency_group,
        UPPER(currency) AS currency_upper
    FROM lines
    WHERE effective_from > DATE '2024-03-04'
      AND tier_value < 55
),
kept_keys AS (
    -- Set difference on keys
    SELECT currency
    FROM cleaned
    EXCEPT DISTINCT
    SELECT currency
    FROM price_tiers
    WHERE effective_from >= '2024-01-01'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM cleaned s
    JOIN kept_keys k ON s.currency = k.currency
),
cleaned_price_tiers AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, LOWER(plan) AS plan_lower
    FROM verified
    WHERE currency IN ('THB', 'USD')
)
SELECT
    plan,
    currency,
    currency_group,
    currency_upper,
    plan_lower
FROM cleaned_price_tiers
ORDER BY plan, currency
