WITH exploded AS (
    SELECT
        id,
        effective_from,
        currency,
        EXPLODE(tier_prices) AS element
    FROM price_tiers
),
kept_keys AS (
    SELECT id, currency
    FROM exploded
    EXCEPT ALL
    SELECT id, currency
    FROM price_tiers
    WHERE effective_from <= '2024-04-01'
),
verified AS (
    SELECT s.*
    FROM exploded s
    INNER JOIN kept_keys k ON s.id = k.id AND s.currency = k.currency
),
cleaned AS (
    SELECT *, CASE WHEN currency IN ('USD', 'THB') THEN 'Group A' ELSE 'Other' END AS currency_group
    FROM verified
    WHERE currency IN ('THB', 'USD', 'EUR')
),
scored AS (
    SELECT
        id,
        element,
        currency_group,
        COUNT(element) OVER (PARTITION BY currency) AS currency_count_element,
        CUME_DIST() OVER (ORDER BY element NULLS FIRST) AS element_cume_dist,
        DENSE_RANK() OVER (ORDER BY element NULLS FIRST) AS element_dense_rank,
        CUME_DIST() OVER (PARTITION BY currency ORDER BY element NULLS LAST) AS element_cume_dist_2
    FROM cleaned
)
SELECT
    CASE WHEN element_cume_dist_2 >= 0.73 THEN 'LARGE' ELSE 'SMALL' END AS element_cume_dist_2_tier,
    APPROX_PERCENTILE(element_cume_dist_2, 0.95) AS element_cume_dist_2_p95,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(DISTINCT id) AS distinct_ids_2,
    ROUND(AVG(element), 2) AS avg_element,
    SUM(element_cume_dist) AS total_element_cume_dist,
    COUNT(DISTINCT id) AS distinct_ids_3
FROM scored
GROUP BY 1
HAVING element_cume_dist_2_p95 > 0
ORDER BY element_cume_dist_2_p95
