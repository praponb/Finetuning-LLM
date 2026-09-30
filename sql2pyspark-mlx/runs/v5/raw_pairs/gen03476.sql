WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        DATE_FORMAT(opened_date, 'MMM yyyy') AS opened_date_label,
        DATE_TRUNC('month', opened_date) AS opened_date_period,
        COALESCE(region, 'OTHER') AS region_clean
    FROM stores
    WHERE city IN ('Bangkok', 'New York')
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        region,
        opened_date_label,
        region_clean,
        ROW_NUMBER() OVER w_grp AS seq_no,
        CUME_DIST() OVER (PARTITION BY city ORDER BY sqft DESC NULLS FIRST) AS sqft_cume_dist,
        SUM(sqft) OVER w_recent AS cumulative_sqft,
        LEAD(sqft, 1, 0) OVER (PARTITION BY region ORDER BY opened_date, id) AS next_sqft,
        SUM(sqft) OVER w_hist AS sqft_sum_90d
    FROM base
    WHERE TO_DATE(opened_date_period) >= '2024-08-01'
    WINDOW w_grp AS (PARTITION BY region ORDER BY sqft DESC, id), w_recent AS (PARTITION BY city, region ORDER BY opened_date_period, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_hist AS (ORDER BY CAST(opened_date_period AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW)
),
kept_keys AS (
    -- Set difference on keys
    SELECT id, region
    FROM metrics
    EXCEPT
    SELECT id, region
    FROM stores
    WHERE store_name LIKE '%Downtown%'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM metrics s
    JOIN kept_keys k ON s.id = k.id AND s.region = k.region
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM reconciled s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory x
            WHERE x.store_id = s.id
              AND x.product_id IN (1, 2, 5, 8)
        )
)
SELECT
    id,
    region,
    seq_no,
    cumulative_sqft,
    next_sqft,
    sqft_sum_90d,
    CASE WHEN region RLIKE '^(South|East)$' THEN 'CORE' WHEN region RLIKE '^(North|West)$' THEN 'PRIORITY' ELSE 'OTHER_ZONE' END AS region_zone
FROM eligible
ORDER BY region DESC
