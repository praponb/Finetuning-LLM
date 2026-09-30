WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        currency,
        element
    FROM price_tiers
    LATERAL VIEW EXPLODE(discounts) x AS element
)
-- Multi-level aggregation
SELECT
    currency,
    CASE WHEN element >= 68 THEN 'PREMIUM' ELSE 'STANDARD' END AS element_tier,
    ROUND(SUM(element) / NULLIF(COUNT(CASE WHEN currency = 'USD' THEN 1 END), 0), 2) AS element_per_usd,
    AVG(element) FILTER (WHERE currency = 'EUR') AS eur_avg_element,
    PERCENTILE_APPROX(element, 0.9) AS element_p90
FROM exploded
GROUP BY currency, CASE WHEN element >= 68 THEN 'PREMIUM' ELSE 'STANDARD' END
