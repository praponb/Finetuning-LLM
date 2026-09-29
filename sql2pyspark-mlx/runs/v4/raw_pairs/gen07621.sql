WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ROUND(discount * 0.9, 2) AS discount_adjusted,
        COALESCE(channel, 'NONE') AS channel_clean
    FROM orders
    WHERE store_id IS NOT NULL
      AND store_id IN (3, 21, 22)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(discount_adjusted) OVER (PARTITION BY customer_id, channel) AS customer_id_sum_discount_adjusted,
        COUNT(discount) OVER (PARTITION BY store_id, customer_id) AS store_id_count_discount,
        NTILE(5) OVER (PARTITION BY channel ORDER BY discount, id) AS discount_bucket
    FROM valid
),
with_latest AS (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.order_item_unit_price
    FROM with_windows s
    LEFT JOIN LATERAL (
            SELECT c.unit_price AS order_item_unit_price
            FROM order_items c
            WHERE c.order_id = s.id
            ORDER BY c.unit_price DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        store_id,
        order_date,
        status,
        channel,
        amount,
        discount,
        shipping_fee,
        discount_adjusted,
        channel_clean,
        customer_id_sum_discount_adjusted,
        store_id_count_discount,
        discount_bucket,
        order_item_unit_price,
        LAST_VALUE(discount_bucket, TRUE) OVER (PARTITION BY customer_id ORDER BY discount_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_discount_bucket,
        LAST_VALUE(discount) IGNORE NULLS OVER (PARTITION BY channel, customer_id ORDER BY store_id_count_discount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_discount,
        RANK() OVER (PARTITION BY customer_id ORDER BY shipping_fee DESC NULLS LAST) AS shipping_fee_rank,
        CORR(shipping_fee, amount) OVER w_hist AS corr_shipping_fee_amount
    FROM with_latest
    WINDOW w_hist AS (PARTITION BY store_id)
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        customer_id,
        store_id,
        order_date,
        status,
        channel,
        amount,
        discount,
        shipping_fee,
        discount_adjusted,
        channel_clean,
        customer_id_sum_discount_adjusted,
        store_id_count_discount,
        discount_bucket,
        order_item_unit_price,
        last_known_discount_bucket,
        last_known_discount,
        shipping_fee_rank,
        corr_shipping_fee_amount,
        SUM(discount) OVER (PARTITION BY channel ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_discount,
        DENSE_RANK() OVER (ORDER BY store_id_count_discount NULLS LAST) AS store_id_count_discount_dense_rank,
        AVG(store_id_count_discount) OVER (PARTITION BY customer_id ORDER BY shipping_fee_rank, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS store_id_count_discount_avg_3,
        LEAD(last_known_discount_bucket, 2) OVER (PARTITION BY status, store_id ORDER BY order_item_unit_price, id) AS next_last_known_discount_bucket,
        FIRST_VALUE(discount_bucket) IGNORE NULLS OVER (PARTITION BY status ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_discount_bucket
    FROM metrics
),
attributed AS (
    -- Non-equi join on a validity window
    SELECT
        src.*,
        pr.id AS promo_id,
        pr.promo_code,
        pr.discount_pct
    FROM enriched src
    LEFT JOIN promotions pr
        ON src.channel <=> pr.channel
       AND src.order_date BETWEEN pr.starts_on AND pr.ends_on
)
SELECT
    id,
    store_id,
    order_date,
    status,
    channel,
    amount,
    discount,
    discount_adjusted,
    customer_id_sum_discount_adjusted,
    CASE WHEN status RLIKE '^(RETURNED|COMPLETED)$' THEN 'OTHER_ZONE' WHEN status RLIKE '^(PENDING|CANCELLED)$' THEN 'CORE' ELSE 'GROUP_B' END AS status_zone
FROM attributed
ORDER BY id NULLS LAST;
