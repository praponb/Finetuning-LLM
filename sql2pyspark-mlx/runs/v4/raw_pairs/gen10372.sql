WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        attr_key,
        attr_value
    FROM carts
    LATERAL VIEW EXPLODE(attributes) li AS attr_key, attr_value
)
SELECT
    s.id,
    s.customer_id,
    s.attr_key,
    (SELECT AVG(x.monthly_fee) FROM subscriptions x WHERE x.customer_id = s.customer_id) AS avg_subscription_monthly_fee
FROM exploded s
ORDER BY avg_subscription_monthly_fee DESC NULLS FIRST, id DESC;
