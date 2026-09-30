SELECT shipping_fee, order_date, discount - shipping_fee AS discount_minus_shipping_fee
FROM orders
WHERE shipping_fee BETWEEN 32 AND 39 AND customer_id IS NULL
ORDER BY shipping_fee DESC;
