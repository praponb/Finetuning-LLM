WITH not_flagged AS (
    SELECT s.*
    FROM stores s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders c
            WHERE c.store_id = s.id
              AND c.order_date >= s.opened_date
        )
),
valid AS (
    SELECT
        id,
        store_name,
        city,
        region,
        sqft,
        CAST(sqft AS STRING) AS sqft_num,
        CONCAT_WS('-', city, store_name) AS city_store_name_key
    FROM not_flagged
    WHERE opened_date > DATE '2023-09-11'
      AND region IS NOT NULL
),
scored AS (
    SELECT
        *,
        AVG(sqft) OVER w_hist AS last4_avg_sqft,
        COUNT(sqft) OVER w_hist AS last4_count_sqft,
        MIN(sqft) OVER w_hist AS last4_min_sqft,
        SUM(sqft) OVER w_hist AS last4_sum_sqft
    FROM valid
    WINDOW w_hist AS (PARTITION BY region, city ORDER BY sqft, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
),
matched_keys AS (
    SELECT region, city
    FROM scored
    EXCEPT ALL
    SELECT region, city
    FROM stores
    WHERE sqft < 7895
),
verified AS (
    SELECT s.*
    FROM scored s
    INNER JOIN matched_keys k ON s.region = k.region AND s.city = k.city
),
enriched AS (
    SELECT
        region,
        city_store_name_key,
        last4_count_sqft,
        last4_sum_sqft,
        ROUND(last4_avg_sqft / SUM(last4_avg_sqft) OVER (PARTITION BY region, city), 4) AS last4_avg_sqft_share,
        ROUND(sqft / SUM(sqft) OVER (PARTITION BY city), 4) AS sqft_share
    FROM verified
)
SELECT
    CASE WHEN sqft_share >= 0.78 THEN 'HIGH' ELSE 'LOW' END AS sqft_share_tier,
    region,
    SUM(CASE WHEN region = 'East' THEN last4_count_sqft ELSE 0 END) AS east_last4_count_sqft,
    SUM(CASE WHEN region = 'East' THEN last4_sum_sqft ELSE 0 END) AS east_last4_sum_sqft,
    ROUND(AVG(last4_avg_sqft_share), 2) AS avg_last4_avg_sqft_share,
    ROUND(AVG(last4_sum_sqft), 2) AS avg_last4_sum_sqft,
    COUNT(DISTINCT region) AS distinct_regions,
    RANK() OVER (PARTITION BY CASE WHEN sqft_share >= 0.78 THEN 'HIGH' ELSE 'LOW' END ORDER BY SUM(CASE WHEN region = 'East' THEN last4_sum_sqft ELSE 0 END) DESC) AS east_last4_sum_sqft_rank
FROM enriched
GROUP BY CASE WHEN sqft_share >= 0.78 THEN 'HIGH' ELSE 'LOW' END, region
HAVING ROUND(AVG(last4_sum_sqft), 2) < 1000000
ORDER BY distinct_regions DESC NULLS LAST;
