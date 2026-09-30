WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        currency,
        val
    FROM price_tiers
    LATERAL VIEW EXPLODE(discounts) x AS val
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        effective_from,
        currency,
        val,
        ROUND(val * 100, 2) AS adjusted_val
    FROM lines
    WHERE val <= 47
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MAX(val) OVER (PARTITION BY currency) AS currency_max_val,
        RANK() OVER (ORDER BY adjusted_val DESC NULLS LAST) AS adjusted_val_rank
    FROM filtered
),
filtered_price_tiers AS (
    -- Basic cleansing and defaults
    SELECT
        currency,
        val,
        currency_max_val,
        adjusted_val_rank,
        CEIL(val) AS val_whole
    FROM enriched
    WHERE effective_from >= TO_DATE('2024-06-13')
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        currency,
        currency_max_val,
        adjusted_val_rank,
        val_whole,
        CASE WHEN val >= 71 THEN 'High' WHEN val >= 26 THEN 'Medium' ELSE 'Low' END AS val_band,
        adjusted_val_rank + currency_max_val AS adjusted_val_rank_plus_currency_max_val
    FROM filtered_price_tiers
    WHERE currency_max_val IS NOT NULL
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        RANK() OVER (PARTITION BY currency ORDER BY currency_max_val DESC) AS currency_max_val_rank,
        PERCENT_RANK() OVER (ORDER BY adjusted_val_rank DESC NULLS LAST) AS adjusted_val_rank_percent_rank,
        MAX(val_whole) OVER (PARTITION BY currency) AS currency_max_val_whole
    FROM cleaned
),
cleaned_price_tiers AS (
    -- Basic cleansing and defaults
    SELECT *, adjusted_val_rank - val_whole AS adjusted_val_rank_minus_val_whole
    FROM metrics
    WHERE currency <> 'EUR'
)
-- Aggregate with conditional metrics
SELECT
    currency,
    COUNT(*) AS event_count,
    COUNT(CASE WHEN currency = 'EUR' THEN 1 END) AS eur_count,
    MAX(val_whole) FILTER (WHERE currency = 'USD') AS usd_max_val_whole,
    AVG(val_whole) AS avg_val_whole,
    MAX(adjusted_val_rank_minus_val_whole) FILTER (WHERE currency = 'THB') AS thb_max_adjusted_val_rank_minus_val_whole,
    COUNT(*) FILTER (WHERE currency = 'EUR') AS eur_rows
FROM cleaned_price_tiers
GROUP BY currency
ORDER BY eur_rows
