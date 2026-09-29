WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, SUBSTRING(region, 1, 2) AS region_prefix
    FROM stores
    WHERE opened_date >= TO_DATE('2023-04-24')
      AND region = 'Central'
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(sqft) OVER w_running AS next_sqft,
        LAG(sqft) OVER w_running AS prev_sqft,
        ROW_NUMBER() OVER w_running AS event_seq,
        SUM(sqft) OVER (PARTITION BY region, city ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_sqft
    FROM filtered
    WINDOW w_running AS (PARTITION BY region ORDER BY opened_date, id)
),
latest_match AS (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.city,
        s.region,
        s.region_prefix,
        s.prev_sqft,
        latest.store_sale_units,
        latest.store_sale_revenue
    FROM scored s
    LEFT JOIN LATERAL (
            SELECT c.units AS store_sale_units, c.revenue AS store_sale_revenue
            FROM store_sales c
            WHERE c.store_id = s.id
              AND c.sale_date <= s.opened_date
            ORDER BY c.sale_date DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY region, city ORDER BY store_sale_units, id) AS latest_rank
        FROM latest_match
    ) x
    WHERE latest_rank = 1
),
scored_stores AS (
    -- Ranking and gap features
    SELECT
        *,
        ROUND(store_sale_revenue / SUM(store_sale_revenue) OVER (PARTITION BY region), 4) AS store_sale_revenue_share,
        ROW_NUMBER() OVER (PARTITION BY region ORDER BY latest_rank, id) AS event_seq,
        CUME_DIST() OVER (PARTITION BY city ORDER BY store_sale_units NULLS FIRST) AS store_sale_units_cume_dist,
        MAX(prev_sqft) OVER (PARTITION BY region) AS region_max_prev_sqft
    FROM deduped
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM scored_stores s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales ch
            WHERE ch.store_id = s.id
              AND ch.store_id = 2
        )
)
-- Running and moving aggregates
SELECT
    id,
    city,
    region,
    region_prefix,
    store_sale_units,
    store_sale_units_cume_dist,
    region_max_prev_sqft,
    NTILE(10) OVER (PARTITION BY city ORDER BY store_sale_revenue, id) AS store_sale_revenue_bucket,
    AVG(store_sale_units) OVER (PARTITION BY region ORDER BY store_sale_units_cume_dist, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS store_sale_units_avg_5_prior
FROM not_flagged
