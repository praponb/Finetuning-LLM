SELECT *, ROUND(shipping_fee, 0) AS shipping_fee_rounded FROM orders WHERE channel = 'store' AND (order_date >= '2025-01-01' OR status IN ('SHIPPED', 'COMPLETED'));
