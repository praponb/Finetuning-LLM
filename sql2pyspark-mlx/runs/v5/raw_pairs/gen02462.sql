WITH latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM stores
    QUALIFY ROW_NUMBER() OVER (PARTITION BY region ORDER BY sqft, id) = 1
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        MAX(sqft) OVER (PARTITION BY region ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_sqft,
        AVG(sqft) OVER (PARTITION BY region ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_sqft,
        COUNT(sqft) OVER (PARTITION BY region ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_sqft,
        SUM(sqft) OVER (PARTITION BY region ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_sqft
    FROM latest
    WHERE LOWER(TRIM(store_name)) = 'airport kiosk'
),
with_last AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.store_name,
        s.city,
        s.opened_date,
        s.running_max_sqft,
        s.running_count_sqft,
        x.inventory_last_restock,
        x.quantity
    FROM metrics s
    JOIN LATERAL (
            SELECT c.last_restock AS inventory_last_restock, c.quantity
            FROM inventory c
            WHERE c.store_id = s.id
            ORDER BY c.last_restock DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
metrics_stores AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(running_count_sqft) OVER (PARTITION BY city ORDER BY quantity, id) AS prev_running_count_sqft,
        LAG(running_max_sqft) OVER (PARTITION BY city ORDER BY quantity, id) AS prev_running_max_sqft,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY quantity, id) AS event_seq,
        AVG(running_count_sqft) OVER (PARTITION BY city ORDER BY quantity, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS running_count_sqft_avg_5_prior
    FROM with_last
),
latest_match AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.inventory_quantity,
        top1.last_restock
    FROM metrics_stores s
    JOIN LATERAL (
            SELECT c.quantity AS inventory_quantity, c.last_restock
            FROM inventory c
            WHERE c.store_id = s.id
            ORDER BY c.last_restock DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
with_latest AS (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.city,
        s.running_max_sqft,
        s.running_count_sqft,
        s.quantity,
        s.prev_running_count_sqft,
        s.running_count_sqft_avg_5_prior,
        latest.shipping_fee,
        latest.order_channel
    FROM latest_match s
    LEFT JOIN LATERAL (
            SELECT c.shipping_fee, c.channel AS order_channel
            FROM orders c
            WHERE c.store_id = s.id
              AND c.order_date <= s.opened_date
            ORDER BY c.order_date DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM with_latest
    QUALIFY RANK() OVER (PARTITION BY city ORDER BY running_max_sqft DESC) = 1
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN running_count_sqft_avg_5_prior >= 15 THEN 'PREMIUM' ELSE 'STANDARD' END AS running_count_sqft_avg_5_prior_tier,
    ROUND(AVG(running_max_sqft), 2) AS avg_running_max_sqft,
    APPROX_PERCENTILE(shipping_fee, ARRAY(0.25, 0.5, 0.75)) AS shipping_fee_quartiles,
    ROUND(SUM(running_count_sqft_avg_5_prior) / NULLIF(COUNT(CASE WHEN city = 'Bangkok' THEN 1 END), 0), 2) AS running_count_sqft_avg_5_prior_per_bangkok,
    MIN(running_max_sqft) AS min_running_max_sqft
FROM ranked
GROUP BY running_count_sqft_avg_5_prior_tier
ORDER BY running_count_sqft_avg_5_prior_per_bangkok;
