WITH valid AS (
    SELECT
        *,
        TRIM(city) clean_city,
        CONCAT(store_name, ', ', region) store_name_region
    FROM stores
    WHERE region = 'East'
),
matched_keys AS (
    SELECT region
    FROM valid
    EXCEPT ALL
    SELECT region
    FROM stores
    WHERE opened_date < '2024-12-01'
),
reconciled AS (
    SELECT s.*
    FROM valid s
    INNER JOIN matched_keys k ON s.region = k.region
),
base AS (
    SELECT *, LENGTH(region) region_length
    FROM reconciled
    WHERE region IS NOT NULL
),
metrics AS (
    SELECT
        *,
        RANK() OVER (ORDER BY region_length DESC) region_length_rank,
        DENSE_RANK() OVER (ORDER BY sqft NULLS FIRST) sqft_dense_rank,
        RANK() OVER (ORDER BY sqft) sqft_rank,
        MIN(region_length) OVER (PARTITION BY region) region_min_region_length,
        CUME_DIST() OVER (PARTITION BY city ORDER BY region_length DESC NULLS FIRST) region_length_cume_dist
    FROM base
)
SELECT
    *,
    MAX(region_length_rank) OVER (PARTITION BY city) city_max_region_length_rank,
    ROUND(sqft / SUM(sqft) OVER (PARTITION BY city), 4) sqft_share,
    CUME_DIST() OVER (PARTITION BY city ORDER BY sqft_dense_rank NULLS LAST) sqft_dense_rank_cume_dist,
    COUNT(region_length_rank) OVER (PARTITION BY city, region) city_count_region_length_rank,
    PERCENT_RANK() OVER (PARTITION BY city, region ORDER BY region_length_cume_dist DESC NULLS LAST) region_length_cume_dist_percent_rank
FROM metrics;
