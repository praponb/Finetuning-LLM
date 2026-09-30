WITH enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY city ORDER BY sqft) AS sqft_percent_rank,
        FIRST_VALUE(sqft) IGNORE NULLS OVER (PARTITION BY region ORDER BY sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_sqft,
        LEAD(sqft, 1) OVER (PARTITION BY region ORDER BY opened_date, id) AS next_sqft,
        LEAD(sqft, 2) OVER (PARTITION BY city ORDER BY opened_date, id) AS next_sqft_2
    FROM stores
),
with_latest AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.inventory_quantity
    FROM enriched s
    LEFT JOIN LATERAL (
            SELECT c.quantity AS inventory_quantity
            FROM inventory c
            WHERE c.store_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
enriched_stores AS (
    -- Running and moving aggregates
    SELECT
        id,
        region,
        opened_date,
        sqft,
        next_sqft,
        next_sqft_2,
        LAST_VALUE(next_sqft) IGNORE NULLS OVER (ORDER BY sqft_percent_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_next_sqft,
        NTILE(4) OVER (PARTITION BY city, region ORDER BY inventory_quantity, id) AS inventory_quantity_bucket
    FROM with_latest
    WHERE store_name LIKE 'D%'
),
enriched_stores_2 AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(last_known_next_sqft) OVER w_ordered AS running_count_last_known_next_sqft,
        MIN(last_known_next_sqft) OVER w_ordered AS running_min_last_known_next_sqft,
        SUM(last_known_next_sqft) OVER w_ordered AS running_sum_last_known_next_sqft
    FROM enriched_stores
    WINDOW w_ordered AS (PARTITION BY region ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_last AS (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.region,
        s.next_sqft,
        s.next_sqft_2,
        s.last_known_next_sqft,
        s.running_count_last_known_next_sqft,
        s.running_min_last_known_next_sqft,
        latest.amount,
        latest.order_shipping_fee
    FROM enriched_stores_2 s
    JOIN LATERAL (
            SELECT c.amount, c.shipping_fee AS order_shipping_fee
            FROM orders c
            WHERE c.store_id = s.id
            ORDER BY c.discount DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
-- Multi-level aggregation
SELECT
    region,
    PERCENTILE_APPROX(running_count_last_known_next_sqft, ARRAY(0.25, 0.5, 0.75)) AS running_count_last_known_next_sqft_quartiles,
    SIZE(COLLECT_SET(region)) AS num_regions,
    PERCENTILE_APPROX(order_shipping_fee, 0.95) AS order_shipping_fee_p95,
    SORT_ARRAY(COLLECT_SET(region)) AS region_list
FROM with_last
GROUP BY region;
