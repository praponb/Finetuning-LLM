WITH valid AS (
    SELECT *, GREATEST(shipping_fee, discount) AS greatest_shipping_fee_discount
    FROM orders
    WHERE order_date >= DATE '2023-03-26'
      AND store_id IS NOT NULL
      AND channel = 'web'
),
with_windows AS (
    SELECT
        *,
        AVG(amount) OVER (PARTITION BY status ORDER BY order_date, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS amount_avg_5,
        CORR(amount, discount) OVER (PARTITION BY customer_id, channel) AS corr_amount_discount
    FROM valid
),
not_flagged AS (
    SELECT s.*
    FROM with_windows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items c
            WHERE c.order_id = s.id
              AND c.unit_price BETWEEN 140 AND 360
        )
)
SELECT
    s.id,
    s.customer_id,
    s.order_date,
    s.status,
    s.amount,
    s.discount,
    s.shipping_fee,
    s.amount_avg_5,
    s.corr_amount_discount,
    (SELECT MIN(a2.balance) FROM accounts a2 WHERE a2.customer_id = s.customer_id) AS min_account_balance
FROM not_flagged s;
