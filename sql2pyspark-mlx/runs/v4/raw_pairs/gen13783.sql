WITH matched AS (
    -- Non-equi join on a validity window
    SELECT
        s.*,
        pr.id AS promo_id,
        pr.promo_code,
        pr.discount_pct
    FROM orders s
    LEFT JOIN promotions pr
        ON s.channel = pr.channel
       AND s.order_date >= pr.starts_on
       AND s.order_date < pr.ends_on + INTERVAL 30 DAYS
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM matched s
    WHERE EXISTS (
            SELECT 1
            FROM order_items ch
            WHERE ch.order_id = s.id
              AND ch.quantity > 8
        )
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM kept s
    WHERE EXISTS (
            SELECT 1
            FROM payments x
            WHERE x.order_id = s.id
              AND x.paid_at < s.order_date
        )
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
        promo_id,
        promo_code,
        discount_pct,
        NTILE(10) OVER (ORDER BY shipping_fee DESC, id, promo_id) AS shipping_fee_bucket,
        NTILE(10) OVER (PARTITION BY customer_id ORDER BY shipping_fee, id, promo_id) AS shipping_fee_bucket_2,
        AVG(shipping_fee) OVER (ORDER BY shipping_fee, id, promo_id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS shipping_fee_avg_2
    FROM not_flagged
),
enriched_orders AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        FIRST_VALUE(shipping_fee_bucket_2, TRUE) OVER (ORDER BY discount_pct, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_shipping_fee_bucket_2,
        ROUND(discount_pct / SUM(discount_pct) OVER (PARTITION BY store_id, channel), 4) AS discount_pct_share,
        AVG(amount) OVER (PARTITION BY channel ORDER BY promo_id, id, promo_id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS amount_avg_5_prior
    FROM enriched
),
matched_keys AS (
    -- Set difference on keys
    SELECT status, store_id
    FROM enriched_orders
    EXCEPT DISTINCT
    SELECT status, store_id
    FROM orders
    WHERE discount > 0.26
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched_orders s
    JOIN matched_keys k ON s.status = k.status AND s.store_id = k.store_id
)
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
    promo_id,
    promo_code,
    discount_pct,
    shipping_fee_bucket,
    shipping_fee_bucket_2,
    shipping_fee_avg_2,
    first_known_shipping_fee_bucket_2,
    discount_pct_share,
    amount_avg_5_prior,
    ROUND(shipping_fee_bucket / SUM(shipping_fee_bucket) OVER (PARTITION BY store_id), 4) AS shipping_fee_bucket_share,
    DENSE_RANK() OVER (ORDER BY discount_pct_share NULLS FIRST) AS discount_pct_share_dense_rank,
    SUM(shipping_fee_bucket_2) OVER (PARTITION BY status) AS status_sum_shipping_fee_bucket_2,
    ROUND(promo_id / SUM(promo_id) OVER (PARTITION BY customer_id), 4) AS promo_id_share
FROM kept_rows;
