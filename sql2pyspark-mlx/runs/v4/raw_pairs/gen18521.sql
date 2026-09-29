WITH with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        ROUND(sqft / SUM(sqft) OVER (PARTITION BY city), 4) AS sqft_share,
        SUM(sqft) OVER (PARTITION BY city ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_sqft,
        LEAD(sqft, 1) OVER (PARTITION BY region ORDER BY sqft, id) AS next_sqft
    FROM stores
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_windows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory c
            WHERE c.store_id = s.id
              AND c.last_restock < s.opened_date
        )
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        sqft_share,
        cumulative_sqft,
        next_sqft,
        LEAD(sqft_share, 1) OVER (PARTITION BY region ORDER BY sqft_share, id) AS next_sqft_share,
        SUM(next_sqft) OVER (PARTITION BY region) AS region_sum_next_sqft,
        LEAD(sqft_share, 1) OVER w_ordered AS next_sqft_share_2
    FROM not_flagged
    WINDOW w_ordered AS (PARTITION BY city, region ORDER BY sqft_share, id)
),
by_type AS (
    -- Native SQL PIVOT
    SELECT *
    FROM (
        SELECT
            city,
            region,
            region_sum_next_sqft
        FROM metrics
    ) src
    PIVOT (
        SUM(region_sum_next_sqft) AS amount
        FOR region IN ('South', 'North', 'West')
    )
)
SELECT
    city,
    North,
    West
FROM by_type
ORDER BY North DESC;
