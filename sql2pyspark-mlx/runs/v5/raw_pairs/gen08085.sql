WITH cleaned AS (
    SELECT
        id,
        customer_id,
        store_id,
        status,
        amount,
        shipping_fee,
        ROUND(discount, 2) AS discount_rounded,
        shipping_fee + discount AS shipping_fee_plus_discount
    FROM orders
    WHERE store_id IS NULL
),
matched_keys AS (
    SELECT status, id
    FROM cleaned
    EXCEPT ALL
    SELECT status, id
    FROM orders
    WHERE YEAR(order_date) = 2023
),
reconciled AS (
    SELECT s.*
    FROM cleaned s
    INNER JOIN matched_keys k ON s.status = k.status AND s.id = k.id
),
scored AS (
    SELECT
        *,
        CORR(shipping_fee, amount) OVER (PARTITION BY status) AS corr_shipping_fee_amount,
        PERCENT_RANK() OVER (PARTITION BY status, customer_id ORDER BY amount NULLS LAST) AS amount_percent_rank,
        CORR(shipping_fee, amount) OVER (PARTITION BY status, customer_id) AS corr_shipping_fee_amount_2
    FROM reconciled
),
eligible AS (
    SELECT s.*
    FROM scored s
    WHERE NOT EXISTS (
            SELECT 1
            FROM shipments ch
            WHERE ch.order_id = s.id
              AND ch.shipped_date BETWEEN '2024-02-01' AND '2025-04-01'
        )
),
not_flagged AS (
    SELECT s.*
    FROM eligible s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items c
            WHERE c.order_id = s.id
              AND c.unit_price BETWEEN 140 AND 390
        )
),
not_flagged_orders AS (
    SELECT s.*
    FROM not_flagged s
    WHERE NOT EXISTS (
            SELECT 1
            FROM payments c
            WHERE c.order_id = s.id
              AND c.method NOT IN ('promptpay', 'card')
        )
)
SELECT
    s.id,
    s.customer_id,
    s.amount,
    s.shipping_fee_plus_discount,
    s.corr_shipping_fee_amount,
    s.amount_percent_rank,
    s.corr_shipping_fee_amount_2,
    (SELECT MIN(t2.amount) FROM orders t2 WHERE t2.customer_id = s.customer_id) AS min_order_amount
FROM not_flagged_orders s
ORDER BY id DESC, shipping_fee_plus_discount DESC
