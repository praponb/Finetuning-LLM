WITH flattened AS (
    SELECT
        id,
        customer_id,
        created_at,
        item.qty,
        item.promo
    FROM (
        SELECT
            id,
            customer_id,
            created_at,
            EXPLODE_OUTER(items) AS item
        FROM carts
    ) t
)
SELECT
    s.id,
    s.customer_id,
    s.created_at,
    s.qty,
    (SELECT SUM(s2.monthly_fee) FROM subscriptions s2 WHERE s2.customer_id = s.customer_id) AS sum_subscription_monthly_fee
FROM flattened s
ORDER BY sum_subscription_monthly_fee DESC
