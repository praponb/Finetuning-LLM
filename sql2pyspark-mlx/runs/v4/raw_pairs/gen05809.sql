WITH kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM stores s
    WHERE EXISTS (
            SELECT 1
            FROM inventory c
            WHERE c.store_id = s.id
              AND c.quantity <> 203
        )
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        city,
        region,
        opened_date,
        sqft,
        LENGTH(region) AS region_length,
        UPPER(region) AS region_upper
    FROM kept
    WHERE region IS NOT NULL
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(10) OVER (ORDER BY region_length DESC, id ASC) AS region_length_bucket,
        NTILE(4) OVER (PARTITION BY city ORDER BY region_length DESC, id ASC) AS region_length_bucket_2,
        SUM(region_length) OVER (PARTITION BY region, city ORDER BY opened_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_region_length
    FROM filtered
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(sqft, 2, 0) OVER (PARTITION BY region ORDER BY region_length ASC, id ASC) AS prev_sqft,
        LAG(region_length_bucket_2, 1) OVER (PARTITION BY region ORDER BY region_length_bucket ASC, id ASC) AS prev_region_length_bucket_2
    FROM metrics
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY region ORDER BY running_region_length DESC, id ASC) AS dedup_rank
        FROM enriched
    ) x
    WHERE dedup_rank = 1
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM ranked
    QUALIFY RANK() OVER (PARTITION BY region, city ORDER BY region_length_bucket_2 DESC NULLS LAST) = 1
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        city,
        region,
        opened_date,
        sqft,
        region_length,
        region_length_bucket,
        region_length_bucket_2,
        prev_sqft,
        prev_region_length_bucket_2,
        dedup_rank,
        CASE WHEN city IN ('New York', 'London') THEN 'Priority' ELSE 'Other' END AS city_group
    FROM latest
    WHERE prev_region_length_bucket_2 <> 4
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        city,
        region,
        region_length_bucket_2
    FROM base
) src
PIVOT (
    AVG(region_length_bucket_2) AS total
    FOR region IN ('South' AS south, 'Central' AS central)
)
