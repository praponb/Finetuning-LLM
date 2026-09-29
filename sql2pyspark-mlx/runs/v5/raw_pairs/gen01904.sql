SELECT
    id,
    status,
    shipping_fee,
    CEIL(discount) AS discount_whole
FROM orders;
