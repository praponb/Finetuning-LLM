WITH lines AS (
    -- Unnest the array into one row per element
    SELECT id, EXPLODE(tier_limits) AS element
    FROM price_tiers
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, CAST(element AS DOUBLE) AS element_str
    FROM lines
    WHERE element BETWEEN 46 AND 54
),
valid AS (
    -- Keep recent, valid rows
    SELECT *, ROUND(element, 1) AS element_rounded
    FROM cleaned
    WHERE element IS NULL
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        element,
        RANK() OVER (ORDER BY element_str) AS element_str_rank
    FROM valid
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN element_str_rank >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END AS element_str_rank_tier,
        ROUND(AVG(element_str_rank), 2) AS avg_element_str_rank,
        PERCENTILE_APPROX(element_str_rank, 0.95) AS element_str_rank_p95,
        COUNT(*) AS cnt,
        PERCENTILE_APPROX(element_str_rank, ARRAY(0.25, 0.5, 0.75)) AS element_str_rank_quartiles,
        PERCENTILE_APPROX(element, ARRAY(0.25, 0.5, 0.75)) AS element_quartiles,
        COUNT(*) AS event_count
    FROM enriched
    GROUP BY CASE WHEN element_str_rank >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END
)
SELECT
    element_str_rank_tier,
    avg_element_str_rank,
    element_str_rank_quartiles,
    element_quartiles,
    event_count
FROM grouped
ORDER BY avg_element_str_rank NULLS LAST
