WITH flattened AS (
    SELECT
        id,
        plan,
        effective_from,
        currency,
        element
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_limits) t AS element
),
enriched AS (
    SELECT
        id,
        plan,
        currency,
        element,
        RANK() OVER (ORDER BY element DESC) AS element_rank,
        PERCENT_RANK() OVER (ORDER BY element NULLS FIRST) AS element_percent_rank
    FROM flattened
),
valid AS (
    SELECT
        id,
        currency,
        element,
        CONCAT(currency, ' ', plan) AS currency_plan,
        COALESCE(element_percent_rank, 0) AS element_percent_rank_filled
    FROM enriched
    WHERE element = 24
),
filtered AS (
    SELECT
        id,
        currency,
        element,
        currency_plan,
        element_percent_rank_filled,
        element_percent_rank_filled * element AS element_percent_rank_filled_times_element
    FROM valid
    WHERE currency IN ('USD', 'THB', 'EUR')
)
SELECT
    CASE WHEN element_percent_rank_filled >= 86 THEN 'HIGH' ELSE 'LOW' END AS element_percent_rank_filled_tier,
    AVG(element_percent_rank_filled_times_element) FILTER (WHERE currency = 'USD') AS usd_avg_element_percent_rank_filled_times_element,
    MAX(element) AS max_element,
    COUNT_IF(currency = 'USD') AS usd_events,
    APPROX_PERCENTILE(element_percent_rank_filled_times_element, 0.95) AS element_percent_rank_filled_times_element_p95
FROM filtered
GROUP BY 1
HAVING AVG(element_percent_rank_filled_times_element) FILTER (WHERE currency = 'USD') > 0;
