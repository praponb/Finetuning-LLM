WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        region,
        opened_date,
        sqft,
        COALESCE(sqft, 0) AS sqft_filled,
        INITCAP(TRIM(city)) AS city_title,
        COALESCE(region, 'N/A') AS region_filled
    FROM stores
    WHERE opened_date >= DATE '2023-08-24'
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM valid s
    WHERE EXISTS (
            SELECT 1
            FROM inventory c
            WHERE c.store_id = s.id
              AND c.last_restock < s.opened_date
        )
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY region ORDER BY sqft DESC, id ASC) AS rank_in_group
    FROM kept
    QUALIFY rank_in_group = 1
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        MIN(sqft_filled) OVER w_recent AS running_min_sqft_filled,
        MAX(sqft_filled) OVER w_recent AS running_max_sqft_filled,
        COUNT(sqft_filled) OVER w_recent AS running_count_sqft_filled,
        AVG(sqft_filled) OVER w_recent AS running_avg_sqft_filled,
        PERCENT_RANK() OVER w_running AS sqft_percent_rank
    FROM deduped
    WINDOW w_recent AS (PARTITION BY region ORDER BY rank_in_group ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_running AS (ORDER BY sqft DESC NULLS LAST)
)
-- Multi-level aggregation
SELECT
    CASE WHEN sqft >= 12768 THEN 'LARGE' ELSE 'SMALL' END AS sqft_tier,
    region,
    DATE_TRUNC('WEEK', opened_date) AS week_start,
    COUNT(*) FILTER (WHERE region = 'West') AS west_rows,
    COUNT(CASE WHEN region = 'Central' THEN 1 END) AS central_count
FROM enriched
GROUP BY ROLLUP (CASE WHEN sqft >= 12768 THEN 'LARGE' ELSE 'SMALL' END, region, DATE_TRUNC('WEEK', opened_date))
ORDER BY sqft_tier DESC NULLS LAST;
