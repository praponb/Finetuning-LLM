WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        TRIM(store_name) AS clean_store_name,
        DATE_FORMAT(opened_date, 'MMM yyyy') AS opened_date_label
    FROM stores
    WHERE region IS NOT NULL
      AND region IS NOT DISTINCT FROM 'North'
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(sqft) OVER (PARTITION BY city, region ORDER BY opened_date, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS sqft_avg_5_prior,
        AVG(sqft) OVER (PARTITION BY city ORDER BY sqft, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS sqft_avg_6,
        AVG(sqft) OVER (PARTITION BY region, city ORDER BY opened_date, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS sqft_avg_3_prior
    FROM base
    WHERE MONTH(opened_date) IN (1, 6, 8)
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        store_name,
        region,
        sqft,
        opened_date_label,
        sqft_avg_3_prior,
        AVG(sqft) OVER w_hist AS running_avg_sqft,
        SUM(sqft) OVER w_hist AS running_sum_sqft,
        MIN(sqft) OVER w_hist AS running_min_sqft,
        NTILE(10) OVER (PARTITION BY region ORDER BY sqft_avg_6, id) AS sqft_avg_6_bucket
    FROM scored
    WINDOW w_hist AS (PARTITION BY city, region ORDER BY sqft_avg_5_prior, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
kept_keys AS (
    -- Set difference on keys
    SELECT id, region
    FROM enriched
    EXCEPT
    SELECT id, region
    FROM stores
    WHERE city = 'New York'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    JOIN kept_keys k ON s.id = k.id AND s.region = k.region
)
-- Aggregate with conditional metrics
SELECT
    region,
    SUM(CASE WHEN region = 'East' THEN running_avg_sqft ELSE 0 END) AS east_running_avg_sqft,
    AVG(sqft) AS avg_sqft
FROM reconciled
GROUP BY region
HAVING SUM(CASE WHEN region = 'East' THEN running_avg_sqft ELSE 0 END) > 0
ORDER BY east_running_avg_sqft DESC;
