WITH exploded AS (
    SELECT
        id,
        effective_from,
        element
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_prices) li AS element
),
scored AS (
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY element) AS element_dense_rank,
        CUME_DIST() OVER (ORDER BY element DESC) AS element_cume_dist
    FROM exploded
),
metrics AS (
    SELECT
        id,
        effective_from,
        DENSE_RANK() OVER (ORDER BY element_cume_dist DESC) AS element_cume_dist_dense_rank,
        RANK() OVER (ORDER BY element DESC) AS element_rank,
        CUME_DIST() OVER (ORDER BY element) AS element_cume_dist_2
    FROM scored
),
valid AS (
    SELECT *, COALESCE(element_cume_dist_2, 0) AS element_cume_dist_2_filled
    FROM metrics
    WHERE effective_from > DATE '2024-05-24'
),
cleaned AS (
    SELECT *, GREATEST(element_cume_dist_2, element_rank) AS greatest_element_cume_dist_2_element_rank
    FROM valid
    WHERE effective_from > TO_DATE('2024-01-28')
      AND element_cume_dist_dense_rank <> 3
)
SELECT
    DATE_TRUNC('MONTH', effective_from) AS activity_month,
    DATE_TRUNC('WEEK', effective_from) AS week_start,
    CASE WHEN element_rank >= 7 THEN 'LARGE' ELSE 'SMALL' END AS element_rank_tier,
    PERCENTILE_APPROX(element_cume_dist_2_filled, 0.95) AS element_cume_dist_2_filled_p95,
    MAX(element_cume_dist_2) AS max_element_cume_dist_2
FROM cleaned
GROUP BY DATE_TRUNC('MONTH', effective_from), DATE_TRUNC('WEEK', effective_from), CASE WHEN element_rank >= 7 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY activity_month DESC
