SELECT
    id,
    discount,
    channel,
    COALESCE(discount, 0) discount_filled,
    shipping_fee / NULLIF(discount, 0) shipping_fee_per_discount
FROM orders
WHERE store_id IS NULL
