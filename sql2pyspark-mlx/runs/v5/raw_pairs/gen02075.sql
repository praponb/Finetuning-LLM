WITH lines AS (
    SELECT
        id,
        plan,
        currency,
        EXPLODE_OUTER(discounts) AS element
    FROM price_tiers
)
SELECT
    CASE WHEN element >= 70 THEN 'HIGH' ELSE 'LOW' END AS element_tier,
    APPROX_PERCENTILE(element, ARRAY(0.25, 0.5, 0.75)) AS element_quartiles,
    COUNT(*) FILTER (WHERE currency = 'EUR') AS eur_rows,
    COUNT(*) AS row_count
FROM lines
GROUP BY CASE WHEN element >= 70 THEN 'HIGH' ELSE 'LOW' END
ORDER BY eur_rows DESC;
