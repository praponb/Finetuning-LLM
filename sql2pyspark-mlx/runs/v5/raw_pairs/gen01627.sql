SELECT *, CASE WHEN shipping_fee >= 38 THEN 'High' WHEN shipping_fee >= 14 THEN 'Medium' ELSE 'Low' END AS shipping_fee_band
FROM orders
WHERE (order_date <= '2024-09-12' AND channel NOT IN ('store', 'web', 'partner')) OR shipping_fee < 26;
