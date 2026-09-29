WITH with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(3) OVER (PARTITION BY channel, status ORDER BY amount DESC, id) amount_bucket,
        CUME_DIST() OVER (PARTITION BY customer_id, store_id ORDER BY amount) amount_cume_dist,
        STDDEV_SAMP(shipping_fee) OVER (PARTITION BY status) status_stddev_samp_shipping_fee
    FROM orders
),
joined AS (
    -- Attach stores attributes
    SELECT
        src.*,
        d.city,
        d.region
    FROM with_windows src
    LEFT JOIN stores d
        ON src.store_id = d.id
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM joined s
    WHERE NOT EXISTS (
            SELECT 1
            FROM shipments ch
            WHERE ch.order_id = s.id
              AND ch.order_id IN (31, 37)
        )
),
enriched AS (
    -- Attach stores attributes
    SELECT src.*, s.city AS store_city
    FROM not_flagged src
    LEFT JOIN stores s
        ON src.store_id = s.id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(amount) OVER (ORDER BY amount_cume_dist, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) amount_avg_2,
        SUM(amount_bucket) OVER (PARTITION BY channel, store_city ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_amount_bucket
    FROM enriched
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY amount_bucket, id) AS rank_in_group
        FROM metrics
    ) t
    WHERE rank_in_group = 1
)
SELECT
    id,
    customer_id,
    channel,
    amount,
    amount_bucket,
    status_stddev_samp_shipping_fee,
    city,
    region,
    store_city
FROM ranked
ORDER BY id DESC
