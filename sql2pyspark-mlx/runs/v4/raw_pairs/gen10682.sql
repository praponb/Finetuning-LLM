WITH not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM stores s
    WHERE EXISTS (
            SELECT 1
            FROM inventory c
            WHERE c.store_id = s.id
              AND c.store_id IN (1, 2, 4)
        )
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY sqft DESC NULLS FIRST, id) AS rank_in_group
    FROM not_flagged
    QUALIFY rank_in_group = 1
),
matched_keys AS (
    -- Set difference on keys
    SELECT id, region
    FROM ranked
    EXCEPT
    SELECT id, region
    FROM stores
    WHERE sqft <= 5604
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM ranked s
    JOIN matched_keys k ON s.id = k.id AND s.region = k.region
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        rank_in_group,
        RANK() OVER w_running AS sqft_rank,
        ROUND(sqft / SUM(sqft) OVER (PARTITION BY region), 4) AS sqft_share,
        DENSE_RANK() OVER (PARTITION BY region ORDER BY sqft NULLS LAST) AS sqft_dense_rank
    FROM kept_rows
    WINDOW w_running AS (PARTITION BY city ORDER BY sqft DESC NULLS LAST)
),
scored_stores AS (
    -- Running and moving aggregates
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        rank_in_group,
        sqft_rank,
        sqft_share,
        sqft_dense_rank,
        RANK() OVER (PARTITION BY city ORDER BY sqft DESC NULLS LAST) AS sqft_rank_2,
        STDDEV_SAMP(sqft_rank) OVER (PARTITION BY city) AS city_stddev_samp_sqft_rank,
        PERCENT_RANK() OVER w_hist AS sqft_rank_percent_rank
    FROM scored
    WINDOW w_hist AS (PARTITION BY region, city ORDER BY sqft_rank DESC NULLS FIRST)
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        rank_in_group,
        sqft_rank,
        sqft_share,
        sqft_dense_rank,
        sqft_rank_2,
        city_stddev_samp_sqft_rank,
        sqft_rank_percent_rank,
        city_stddev_samp_sqft_rank - rank_in_group AS city_stddev_samp_sqft_rank_minus_rank_in_group,
        CAST(rank_in_group AS STRING) AS rank_in_group_num,
        COALESCE(region, 'N/A') AS region_filled
    FROM scored_stores
    WHERE sqft_rank_2 < 7
),
summary AS (
    -- Grouped metrics
    SELECT
        DATE_TRUNC('WEEK', opened_date) AS week_start,
        DATE_TRUNC('MONTH', opened_date) AS activity_month,
        SORT_ARRAY(COLLECT_SET(region)) AS region_list,
        COUNT(CASE WHEN region = 'West' THEN 1 END) AS west_count,
        COUNT(CASE WHEN region = 'Central' THEN 1 END) AS central_count
    FROM base
    GROUP BY DATE_TRUNC('WEEK', opened_date), DATE_TRUNC('MONTH', opened_date)
    HAVING COUNT(CASE WHEN region = 'Central' THEN 1 END) >= 1
)
SELECT
    week_start,
    activity_month,
    west_count
FROM summary
ORDER BY activity_month DESC
