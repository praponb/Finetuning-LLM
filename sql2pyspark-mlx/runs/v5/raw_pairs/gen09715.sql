WITH kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM orders s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items x
            WHERE x.order_id = s.id
              AND x.quantity <= 5
        )
)
SELECT
    s.id,
    s.customer_id,
    s.order_date,
    s.status,
    s.channel,
    s.amount,
    s.discount,
    CASE WHEN status RLIKE '^(SHIPPED|RETURNED)$' THEN 'PRIORITY' ELSE 'GROUP_B' END AS status_zone,
    (SELECT SUM(x.balance) FROM accounts x WHERE x.customer_id = s.customer_id) AS sum_account_balance
FROM kept s
ORDER BY status_zone DESC;
