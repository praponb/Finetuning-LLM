WITH scored AS (
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
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY shipping_fee DESC, id) AS row_seq,
        LEAD(shipping_fee, 1) OVER (PARTITION BY store_id, status ORDER BY shipping_fee, id) AS next_shipping_fee
    FROM orders
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(discount) OVER w_ordered AS discount_avg_6,
        NTILE(3) OVER (ORDER BY discount DESC, id) AS discount_bucket,
        DENSE_RANK() OVER w_hist AS discount_dense_rank
    FROM scored
    WINDOW w_ordered AS (PARTITION BY status, customer_id ORDER BY discount, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), w_hist AS (PARTITION BY customer_id ORDER BY discount NULLS FIRST)
),
with_last AS (
    -- Latest matching payment per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.paid_at,
        top1.method
    FROM with_windows s
    LEFT JOIN LATERAL (
            SELECT c.paid_at, c.method
            FROM payments c
            WHERE c.order_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY next_shipping_fee, id) AS row_num
        FROM with_last
    ) t
    WHERE row_num = 1
)
SELECT
    s.id,
    s.store_id,
    s.order_date,
    s.status,
    s.channel,
    s.amount,
    s.shipping_fee,
    s.row_seq,
    s.next_shipping_fee,
    (SELECT MAX(t2.units) FROM store_sales t2 WHERE t2.store_id = s.store_id) AS max_store_sale_units
FROM latest s
ORDER BY shipping_fee DESC, store_id DESC;
