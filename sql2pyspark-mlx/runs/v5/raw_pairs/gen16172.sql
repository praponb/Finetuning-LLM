WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        UPPER(city) AS city_upper,
        COALESCE(region, 'DIRECT') AS region_filled
    FROM stores
    WHERE opened_date > DATE '2023-05-30'
      AND region IS NOT NULL
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM cleaned s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales x
            WHERE x.store_id = s.id
              AND x.sale_date < s.opened_date
        )
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city, region ORDER BY opened_date NULLS LAST, id) AS rank_in_group
    FROM not_flagged
    QUALIFY rank_in_group = 1
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(sqft, 1) OVER w_grp AS next_sqft,
        SUM(sqft) OVER (PARTITION BY region ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sqft,
        AVG(sqft) OVER (PARTITION BY region ORDER BY opened_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS sqft_avg_6,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY opened_date DESC, id) AS event_seq,
        SUM(sqft) OVER w_running AS cumulative_sqft
    FROM ranked
    WHERE region IS NOT NULL
    WINDOW w_grp AS (PARTITION BY region ORDER BY opened_date, id), w_running AS (ORDER BY rank_in_group, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        region,
        city,
        event_seq
    FROM scored
) src
PIVOT (
    AVG(event_seq) AS value_sum
    FOR city IN ('Bangkok' AS bangkok, 'Chiang Mai' AS chiang_mai, 'London' AS london)
)
