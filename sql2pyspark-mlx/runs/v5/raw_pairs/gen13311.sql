SELECT
    id,
    created_at,
    coupon_codes,
    INLINE(items)
FROM carts;
