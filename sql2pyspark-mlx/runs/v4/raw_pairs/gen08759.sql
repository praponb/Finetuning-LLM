WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        effective_from,
        currency,
        element
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_prices) t AS element
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY currency ORDER BY element) AS element_cume_dist,
        CUME_DIST() OVER (PARTITION BY currency ORDER BY element DESC) AS element_cume_dist_2,
        CUME_DIST() OVER (PARTITION BY currency ORDER BY element DESC NULLS LAST) AS element_cume_dist_3,
        RANK() OVER (PARTITION BY currency ORDER BY element) AS element_rank,
        CUME_DIST() OVER (PARTITION BY currency ORDER BY element) AS element_cume_dist_4
    FROM flattened
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        currency,
        element,
        element_cume_dist_2,
        CASE WHEN element_cume_dist_3 >= 0.66 THEN 'High' WHEN element_cume_dist_3 >= 0.29 THEN 'Medium' ELSE 'Low' END AS element_cume_dist_3_band
    FROM enriched
    WHERE element > 68
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        RANK() OVER (PARTITION BY currency ORDER BY element_cume_dist_2 DESC NULLS FIRST) AS element_cume_dist_2_rank,
        RANK() OVER (PARTITION BY currency ORDER BY element NULLS FIRST) AS element_rank,
        DENSE_RANK() OVER (ORDER BY element DESC NULLS FIRST) AS element_dense_rank,
        RANK() OVER (PARTITION BY currency ORDER BY element DESC NULLS LAST) AS element_rank_2,
        DENSE_RANK() OVER (PARTITION BY currency ORDER BY element) AS element_dense_rank_2
    FROM base
)
SELECT
    id,
    currency,
    element,
    element_cume_dist_2,
    element_cume_dist_3_band,
    element_rank,
    element_dense_rank,
    element_rank_2,
    CASE WHEN currency RLIKE '^(USD|THB)$' THEN 'GROUP_B' ELSE 'GROUP_A' END AS currency_zone
FROM scored
