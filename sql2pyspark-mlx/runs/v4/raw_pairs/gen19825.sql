WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT *, TRIM(store_name) AS clean_store_name
    FROM stores
    WHERE opened_date >= TO_DATE('2023-11-14')
      AND region IS NOT NULL
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        FIRST_VALUE(sqft) IGNORE NULLS OVER (PARTITION BY region ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_sqft,
        DENSE_RANK() OVER (PARTITION BY region ORDER BY sqft) AS sqft_dense_rank,
        NTILE(4) OVER (ORDER BY sqft DESC, id) AS sqft_bucket
    FROM cleaned
),
metrics_stores AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(sqft_bucket) OVER (PARTITION BY region ORDER BY sqft_dense_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_sqft_bucket,
        CUME_DIST() OVER (PARTITION BY city ORDER BY sqft_dense_rank DESC) AS sqft_dense_rank_cume_dist,
        ROW_NUMBER() OVER (PARTITION BY region ORDER BY sqft_bucket DESC, id) AS row_seq
    FROM metrics
),
kept_keys AS (
    -- Set intersection on keys
    SELECT city, region
    FROM metrics_stores
    INTERSECT
    SELECT city, region
    FROM stores
    WHERE region IN ('Central', 'South', 'North')
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM metrics_stores s
    JOIN kept_keys k ON s.city = k.city AND s.region = k.region
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM reconciled s
    WHERE EXISTS (
            SELECT 1
            FROM orders x
            WHERE x.store_id = s.id
              AND x.order_date <= s.opened_date
        )
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY city ORDER BY cumulative_sqft_bucket DESC NULLS LAST) AS cumulative_sqft_bucket_dense_rank,
        DENSE_RANK() OVER (PARTITION BY city ORDER BY sqft_bucket DESC) AS sqft_bucket_dense_rank,
        SUM(sqft) OVER (PARTITION BY region) AS region_sum_sqft
    FROM kept
),
cleaned_stores AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CASE WHEN region_sum_sqft >= 85904 THEN 'High' WHEN region_sum_sqft >= 14042 THEN 'Medium' ELSE 'Low' END AS region_sum_sqft_level,
        sqft_dense_rank_cume_dist * region_sum_sqft AS sqft_dense_rank_cume_dist_times_region_sum_sqft,
        COALESCE(region, 'NONE') AS region_clean
    FROM scored
    WHERE opened_date > TO_DATE('2023-12-30')
      AND region IS NOT NULL
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        city,
        sqft_dense_rank_cume_dist,
        sqft_dense_rank_cume_dist_times_region_sum_sqft
    FROM cleaned_stores
) src
UNPIVOT (
    val FOR metric_name IN (sqft_dense_rank_cume_dist, sqft_dense_rank_cume_dist_times_region_sum_sqft)
);
