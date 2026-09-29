WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_id,
        status,
        amount,
        shipping_fee,
        LAST_DAY(order_date) AS order_date_month_end,
        COALESCE(channel, 'OTHER') AS channel_filled
    FROM orders
    WHERE order_date > DATE '2023-07-03'
      AND customer_id IS NOT NULL
),
enriched AS (
    -- Attach stores attributes
    SELECT
        src.id,
        src.store_id,
        src.order_date_month_end,
        p.region
    FROM valid src
    LEFT JOIN stores p
        ON src.store_id = p.id
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY region, store_id ORDER BY order_date_month_end NULLS FIRST, id) AS dedup_rank
    FROM enriched
    QUALIFY dedup_rank = 1
),
valid_orders AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_date_month_end,
        region,
        ROUND(dedup_rank * 0.85, 2) AS dedup_rank_adjusted,
        COALESCE(region, 'N/A') AS region_clean
    FROM ranked
    WHERE YEAR(order_date_month_end) >= 2025
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(dedup_rank_adjusted) OVER (PARTITION BY region ORDER BY order_date_month_end, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS dedup_rank_adjusted_avg_5_prior,
        SUM(dedup_rank_adjusted) OVER (PARTITION BY region) AS region_sum_dedup_rank_adjusted,
        SUM(dedup_rank_adjusted) OVER (PARTITION BY region ORDER BY dedup_rank_adjusted, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_dedup_rank_adjusted
    FROM valid_orders
    WHERE dedup_rank_adjusted < 43
)
-- Multi-level aggregation
SELECT
    CASE WHEN running_dedup_rank_adjusted >= 338 THEN 'LARGE' ELSE 'SMALL' END AS running_dedup_rank_adjusted_tier,
    ROUND(SUM(running_dedup_rank_adjusted) / NULLIF(COUNT(CASE WHEN region = 'South' THEN 1 END), 0), 2) AS running_dedup_rank_adjusted_per_south,
    AVG(dedup_rank_adjusted) FILTER (WHERE region = 'South') AS south_avg_dedup_rank_adjusted,
    COUNT_IF(region = 'Central') AS central_events
FROM scored
GROUP BY CASE WHEN running_dedup_rank_adjusted >= 338 THEN 'LARGE' ELSE 'SMALL' END
HAVING south_avg_dedup_rank_adjusted > 0
ORDER BY running_dedup_rank_adjusted_per_south DESC
