WITH with_latest AS (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.store_sale_units,
        top1.store_sale_revenue
    FROM stores s
    LEFT JOIN LATERAL (
            SELECT c.units AS store_sale_units, c.revenue AS store_sale_revenue
            FROM store_sales c
            WHERE c.store_id = s.id
            ORDER BY c.units DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        store_sale_units,
        store_sale_revenue,
        ROW_NUMBER() OVER (ORDER BY store_sale_revenue, id) AS event_seq,
        SUM(sqft) OVER (PARTITION BY region ORDER BY store_sale_units, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sqft,
        LAG(sqft, 2) OVER (PARTITION BY city ORDER BY opened_date, id) AS prev_sqft,
        ROW_NUMBER() OVER (PARTITION BY region ORDER BY sqft, id) AS seq_no
    FROM with_latest
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_windows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory ch
            WHERE ch.store_id = s.id
              AND ch.store_id IN (3, 4, 6)
        )
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        store_sale_units,
        store_sale_revenue,
        event_seq,
        running_sqft,
        prev_sqft,
        ROUND(running_sqft / event_seq, 2) AS running_sqft_per_event_seq,
        TRIM(store_name) AS clean_store_name,
        COALESCE(region, 'NONE') AS resolved_region
    FROM not_flagged
    WHERE prev_sqft IS NOT NULL
)
SELECT
    id,
    store_name,
    city,
    region,
    sqft,
    store_sale_revenue,
    running_sqft,
    prev_sqft,
    running_sqft_per_event_seq,
    CASE WHEN region RLIKE '^(South|West)$' THEN 'PRIORITY' ELSE 'OTHER_ZONE' END AS region_zone
FROM cleaned
ORDER BY city DESC NULLS FIRST;
