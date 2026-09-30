WITH valid AS (
    SELECT
        *,
        ROUND(discount * 1.1, 2) AS discount_scaled,
        COALESCE(channel, 'DIRECT') AS channel_clean
    FROM orders
    WHERE order_date >= DATE '2023-07-08'
      AND customer_id IS NOT NULL
      AND shipping_fee >= 20
),
enriched AS (
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
        discount_scaled,
        channel_clean,
        MAX(discount) OVER (PARTITION BY customer_id) AS customer_id_max_discount,
        AVG(discount) OVER (PARTITION BY status ORDER BY amount, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS discount_avg_6_prior,
        ROW_NUMBER() OVER (ORDER BY discount DESC, id) AS seq_no,
        CUME_DIST() OVER (PARTITION BY store_id, status ORDER BY amount DESC NULLS LAST) AS amount_cume_dist,
        NTILE(10) OVER (PARTITION BY status ORDER BY shipping_fee DESC, id) AS shipping_fee_bucket
    FROM valid
),
valid_orders AS (
    SELECT
        *,
        COALESCE(discount, 0) AS discount_filled,
        INITCAP(TRIM(status)) AS status_title,
        COALESCE(channel, 'OTHER') AS channel_clean_2
    FROM enriched
    WHERE discount BETWEEN 0.15 AND 0.39
)
SELECT
    s.id,
    s.customer_id,
    s.order_date,
    s.status,
    s.channel,
    s.amount,
    s.shipping_fee,
    s.discount_scaled,
    s.customer_id_max_discount,
    CASE WHEN channel RLIKE '^(store|partner)$' THEN 'OTHER_ZONE' WHEN channel RLIKE '^(web|mobile)$' THEN 'PRIORITY' ELSE 'GROUP_A' END AS channel_zone,
    (SELECT AVG(x.balance) FROM accounts x WHERE x.customer_id = s.customer_id) AS avg_account_balance
FROM valid_orders s
ORDER BY customer_id DESC NULLS FIRST;
