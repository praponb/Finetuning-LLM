WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        order_date,
        status,
        channel,
        amount,
        discount,
        shipping_fee,
        COALESCE(channel, 'N/A') AS channel_filled,
        COALESCE(channel, 'NONE') AS channel_filled_2
    FROM orders
    WHERE order_date > DATE '2023-06-12'
      AND customer_id IS NOT NULL
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM valid
    QUALIFY ROW_NUMBER() OVER (PARTITION BY channel ORDER BY discount, id) = 1
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER w_grp AS discount_rank,
        LAG(shipping_fee, 1) OVER w_grp_2 AS prev_shipping_fee
    FROM deduped
    WINDOW w_grp AS (ORDER BY discount DESC), w_grp_2 AS (PARTITION BY status, customer_id ORDER BY discount, id)
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        order_date,
        status,
        channel,
        amount,
        shipping_fee,
        AVG(discount_rank) OVER (PARTITION BY customer_id, status ORDER BY discount_rank, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_avg_discount_rank,
        MAX(discount_rank) OVER (PARTITION BY customer_id, status ORDER BY discount_rank, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_max_discount_rank,
        SUM(discount_rank) OVER (PARTITION BY customer_id, status ORDER BY discount_rank, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_sum_discount_rank,
        SUM(prev_shipping_fee) OVER (PARTITION BY channel ORDER BY shipping_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_prev_shipping_fee
    FROM metrics
    WHERE discount_rank <> 4
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        status,
        channel,
        amount,
        shipping_fee,
        last3_avg_discount_rank,
        last3_max_discount_rank,
        last3_sum_discount_rank,
        cumulative_prev_shipping_fee,
        CAST(amount AS INT) AS amount_int,
        ROUND(cumulative_prev_shipping_fee / last3_avg_discount_rank, 2) AS cumulative_prev_shipping_fee_per_last3_avg_discount_rank,
        COALESCE(channel, 'DIRECT') AS channel_clean
    FROM enriched
    WHERE order_date > DATE '2023-02-20'
      AND cumulative_prev_shipping_fee = 158
),
metrics_orders AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        channel,
        amount,
        shipping_fee,
        last3_max_discount_rank,
        cumulative_prev_shipping_fee,
        cumulative_prev_shipping_fee_per_last3_avg_discount_rank,
        channel_clean,
        LAST_VALUE(amount, TRUE) OVER w_ordered AS last_known_amount,
        DENSE_RANK() OVER (PARTITION BY channel ORDER BY shipping_fee DESC) AS shipping_fee_dense_rank
    FROM base
    WINDOW w_ordered AS (PARTITION BY status, channel ORDER BY last3_sum_discount_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    channel,
    amount,
    last3_max_discount_rank,
    channel_clean,
    last_known_amount,
    shipping_fee_dense_rank,
    CASE WHEN channel RLIKE '^(web|partner)$' THEN 'CORE' WHEN channel RLIKE '^(mobile|store)$' THEN 'GROUP_A' ELSE 'EXTENDED' END AS channel_zone
FROM metrics_orders
