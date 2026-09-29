WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        currency,
        val
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_limits) li AS val
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, UPPER(currency) AS currency_upper
    FROM flattened
    WHERE val > 37
),
cleaned_price_tiers AS (
    -- Basic cleansing and defaults
    SELECT *, TRIM(currency) AS clean_currency
    FROM cleaned
    WHERE val BETWEEN 61 AND 70
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        currency,
        val,
        clean_currency,
        UPPER(currency) AS currency_upper_2,
        CEIL(val) AS val_whole
    FROM cleaned_price_tiers
    WHERE val > 65
),
base_price_tiers AS (
    -- Keep recent, valid rows
    SELECT
        val,
        clean_currency,
        currency_upper_2,
        val_whole,
        val * val_whole AS val_times_val_whole
    FROM base
    WHERE currency = 'USD'
)
SELECT clean_currency, val_whole
FROM base_price_tiers
