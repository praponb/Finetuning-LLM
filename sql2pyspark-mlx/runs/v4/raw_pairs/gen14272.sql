SELECT *, COALESCE(shipping_fee, 0) AS shipping_fee_filled, ROUND(amount * 1.15, 2) AS amount_adjusted
FROM orders
WHERE status = 'PENDING' AND (store_id IN (21, 33) OR store_id = 8);
