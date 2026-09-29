WITH with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(sqft) OVER (PARTITION BY region ORDER BY opened_date, id) AS next_sqft,
        ROW_NUMBER() OVER (PARTITION BY region ORDER BY opened_date, id) AS seq_no,
        CUME_DIST() OVER (ORDER BY sqft) AS sqft_cume_dist
    FROM stores
),
with_windows_stores AS (
    -- Ranking and gap features
    SELECT
        id,
        city,
        next_sqft,
        seq_no,
        sqft_cume_dist,
        MAX(sqft_cume_dist) OVER w_recent AS running_max_sqft_cume_dist,
        SUM(sqft_cume_dist) OVER w_recent AS running_sum_sqft_cume_dist,
        COUNT(sqft_cume_dist) OVER w_recent AS running_count_sqft_cume_dist
    FROM with_windows
    WINDOW w_recent AS (PARTITION BY region, city ORDER BY seq_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *, RANK() OVER (PARTITION BY city ORDER BY next_sqft DESC) AS rn
    FROM with_windows_stores
    QUALIFY rn = 1
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id, city
    FROM deduped
    EXCEPT ALL
    SELECT id, city
    FROM stores
    WHERE region IN ('South', 'East')
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM deduped s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.city = k.city
),
stats AS (
    -- Grouped metrics
    SELECT
        CASE WHEN running_max_sqft_cume_dist >= 0.9 THEN 'LARGE' ELSE 'SMALL' END AS running_max_sqft_cume_dist_tier,
        city,
        COUNT(DISTINCT id) AS distinct_ids,
        SORT_ARRAY(COLLECT_SET(city)) AS city_list,
        SORT_ARRAY(COLLECT_SET(city)) AS city_list_2
    FROM reconciled
    GROUP BY CASE WHEN running_max_sqft_cume_dist >= 0.9 THEN 'LARGE' ELSE 'SMALL' END, city
)
SELECT
    running_max_sqft_cume_dist_tier,
    city,
    distinct_ids,
    city_list,
    city_list_2
FROM stats
ORDER BY running_max_sqft_cume_dist_tier
