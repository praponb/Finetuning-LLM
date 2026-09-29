WITH flattened AS (
    SELECT
        id,
        effective_from,
        EXPLODE(tier_limits) AS element
    FROM price_tiers
)
SELECT
    DATE_TRUNC('YEAR', effective_from) AS year_start,
    COUNT(DISTINCT id) AS distinct_ids,
    ROUND(AVG(element), 2) AS avg_element,
    COUNT(*) AS row_count,
    GROUPING_ID() AS grouping_level
FROM flattened
GROUP BY CUBE (DATE_TRUNC('YEAR', effective_from));
