WITH lines AS (
    SELECT
        id,
        currency,
        EXPLODE(discounts) AS val
    FROM price_tiers
),
valid AS (
    SELECT *, CASE WHEN currency IN ('EUR', 'USD') THEN 'Group A' ELSE 'Other' END AS currency_group
    FROM lines
    WHERE val >= 54
),
base AS (
    SELECT *, LENGTH(currency) AS currency_length
    FROM valid
    WHERE val <= 32
),
stats AS (
    SELECT
        CASE WHEN val >= 72 THEN 'LARGE' ELSE 'SMALL' END AS val_tier,
        currency,
        SUM(currency_length) FILTER (WHERE currency = 'USD') AS usd_total_currency_length,
        COUNT(*) FILTER (WHERE currency = 'EUR') AS eur_rows,
        ROUND(AVG(val), 2) AS avg_val,
        ROUND(AVG(currency_length), 2) AS avg_currency_length,
        SORT_ARRAY(COLLECT_SET(currency)) AS currency_list,
        SIZE(COLLECT_SET(currency)) AS num_currencies,
        GROUPING(CASE WHEN val >= 72 THEN 'LARGE' ELSE 'SMALL' END) AS grouping_id
    FROM base
    GROUP BY ROLLUP (CASE WHEN val >= 72 THEN 'LARGE' ELSE 'SMALL' END, currency)
)
SELECT
    currency,
    usd_total_currency_length,
    eur_rows,
    avg_val,
    avg_currency_length,
    currency_list,
    grouping_id
FROM stats
ORDER BY avg_currency_length NULLS FIRST;
