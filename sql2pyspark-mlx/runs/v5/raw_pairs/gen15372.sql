WITH flattened AS (
    SELECT
        id,
        plan,
        element
    FROM price_tiers
    LATERAL VIEW EXPLODE(discounts) e AS element
)
SELECT
    CASE WHEN element >= 88 THEN 'LARGE' ELSE 'SMALL' END AS element_tier,
    plan,
    APPROX_PERCENTILE(element, ARRAY(0.25, 0.5, 0.75)) AS element_quartiles,
    ROUND(AVG(element), 2) AS avg_element,
    MIN(element) AS min_element,
    COUNT(*) FILTER (WHERE plan = 'basic') AS basic_rows,
    COUNT(*) AS num_rows
FROM flattened
GROUP BY 1, 2
HAVING COUNT(*) > 1
ORDER BY min_element ASC;
