WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT id, EXPLODE(discounts) AS element
    FROM price_tiers
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY element DESC NULLS LAST) AS element_dense_rank,
        CUME_DIST() OVER (ORDER BY element NULLS FIRST) AS element_cume_dist,
        DENSE_RANK() OVER (ORDER BY element DESC NULLS LAST) AS element_dense_rank_2,
        RANK() OVER (ORDER BY element NULLS FIRST) AS element_rank
    FROM exploded
),
base AS (
    -- Basic cleansing and defaults
    SELECT *, element + element_dense_rank_2 AS element_plus_element_dense_rank_2
    FROM with_windows
    WHERE element_dense_rank <= 5
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM base
    INTERSECT ALL
    SELECT id
    FROM price_tiers
    WHERE plan <> 'basic'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base s
    JOIN remaining_keys k ON s.id = k.id
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        RANK() OVER w_ordered AS element_plus_element_dense_rank_2_rank,
        RANK() OVER (ORDER BY element_plus_element_dense_rank_2 DESC NULLS FIRST) AS element_plus_element_dense_rank_2_rank_2
    FROM verified
    WHERE element_dense_rank_2 BETWEEN 3 AND 7
    WINDOW w_ordered AS (ORDER BY element_plus_element_dense_rank_2 DESC)
),
base_price_tiers AS (
    -- Keep recent, valid rows
    SELECT
        id,
        element_dense_rank,
        element_cume_dist,
        element_dense_rank_2,
        element_rank,
        element_plus_element_dense_rank_2,
        element_plus_element_dense_rank_2_rank_2,
        ROUND(element_plus_element_dense_rank_2_rank / element_rank, 2) AS element_plus_element_dense_rank_2_rank_per_element_rank,
        ROUND(element_plus_element_dense_rank_2_rank_2, 0) AS element_plus_element_dense_rank_2_rank_2_rounded
    FROM enriched
    WHERE element_plus_element_dense_rank_2_rank IS NOT NULL
),
totals AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN element_cume_dist >= 0.83 THEN 'PREMIUM' ELSE 'STANDARD' END AS element_cume_dist_tier,
        MIN(element_plus_element_dense_rank_2_rank_per_element_rank) AS min_element_plus_element_dense_rank_2_rank_per_element_rank,
        COUNT(DISTINCT id) AS distinct_ids,
        ROUND(AVG(element_plus_element_dense_rank_2_rank_2), 2) AS avg_element_plus_element_dense_rank_2_rank_2,
        COUNT(DISTINCT id) AS distinct_ids_2
    FROM base_price_tiers
    GROUP BY 1
)
SELECT
    element_cume_dist_tier,
    min_element_plus_element_dense_rank_2_rank_per_element_rank,
    distinct_ids,
    avg_element_plus_element_dense_rank_2_rank_2,
    distinct_ids_2
FROM totals
