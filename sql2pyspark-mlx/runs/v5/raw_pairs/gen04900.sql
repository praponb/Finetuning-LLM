WITH base AS (
    SELECT
        id,
        city,
        region,
        opened_date,
        sqft,
        REPLACE(store_name, ' ', '-') store_name_slug
    FROM stores
    WHERE region IS NOT NULL
      AND city <> 'New York'
),
ranked AS (
    SELECT *
    FROM base
    QUALIFY ROW_NUMBER() OVER (PARTITION BY region ORDER BY sqft DESC, id) = 1
),
filtered AS (
    SELECT
        id,
        region,
        opened_date,
        sqft,
        store_name_slug,
        CONCAT(region, '/', city) region_city,
        COALESCE(region, 'NONE') resolved_region
    FROM ranked
    WHERE opened_date >= TO_DATE('2023-06-29')
),
metrics AS (
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY region ORDER BY sqft DESC NULLS LAST) sqft_dense_rank,
        ROUND(sqft / SUM(sqft) OVER (PARTITION BY region), 4) sqft_share,
        DENSE_RANK() OVER w_part sqft_dense_rank_2
    FROM filtered
    WHERE opened_date >= '2025-02-22'
    WINDOW w_part AS (PARTITION BY region ORDER BY sqft DESC NULLS FIRST)
)
SELECT
    id,
    region,
    opened_date,
    sqft,
    store_name_slug,
    region_city,
    resolved_region,
    sqft_share,
    sqft_dense_rank_2
FROM metrics;
