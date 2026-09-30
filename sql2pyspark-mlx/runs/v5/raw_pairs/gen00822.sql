WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT *, CASE WHEN city IN ('Tokyo', 'Bangkok') THEN 'Tier 1' ELSE 'Other' END AS city_group
    FROM stores
    WHERE opened_date >= DATE '2023-09-24'
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MAX(sqft) OVER (PARTITION BY region) AS region_max_sqft,
        FIRST_VALUE(sqft, TRUE) OVER (PARTITION BY region ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_sqft,
        LAST_VALUE(sqft) IGNORE NULLS OVER (ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_sqft
    FROM cleaned
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(last_known_sqft) OVER (PARTITION BY region, city) AS region_count_last_known_sqft,
        NTILE(4) OVER (ORDER BY last_known_sqft DESC, id) AS last_known_sqft_bucket,
        AVG(first_known_sqft) OVER (PARTITION BY city ORDER BY sqft, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS first_known_sqft_avg_2_prior
    FROM with_windows
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY region ORDER BY region_max_sqft DESC NULLS FIRST) AS latest_rank
        FROM metrics
    ) t
    WHERE latest_rank = 1
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        latest_rank * last_known_sqft AS latest_rank_times_last_known_sqft,
        CEIL(first_known_sqft) AS first_known_sqft_whole,
        COALESCE(region, 'NONE') AS resolved_region
    FROM deduped
    WHERE opened_date > TO_DATE('2023-04-20')
      AND region IS DISTINCT FROM 'East'
)
SELECT
    id,
    opened_date,
    sqft,
    city_group,
    region_max_sqft,
    first_known_sqft,
    region_count_last_known_sqft,
    first_known_sqft_avg_2_prior,
    latest_rank_times_last_known_sqft
FROM base
ORDER BY region_max_sqft;
