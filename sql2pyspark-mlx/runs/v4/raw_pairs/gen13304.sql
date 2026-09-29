WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        INLINE(items)
    FROM carts
)
SELECT
    s.id,
    s.customer_id,
    s.created_at,
    s.coupon_codes,
    s.product_id,
    s.price,
    s.promo,
    (SELECT SUM(a2.balance) FROM accounts a2 WHERE a2.customer_id = s.customer_id) AS sum_account_balance
FROM flattened s
ORDER BY created_at DESC;
