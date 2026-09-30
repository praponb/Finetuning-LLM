SELECT
    MONTH(o.order_date) AS order_month,
    CASE WHEN o.shipping_fee >= 30 THEN 'high' WHEN o.shipping_fee >= 15 THEN 'mid' ELSE 'low' END AS shipping_fee_bucket,
    SUM(oi.unit_price) AS total_unit_price,
    ROUND(AVG(oi.unit_price), 2) AS avg_unit_price
FROM order_items oi
LEFT JOIN orders o ON oi.order_id = o.id
GROUP BY MONTH(o.order_date), CASE WHEN o.shipping_fee >= 30 THEN 'high' WHEN o.shipping_fee >= 15 THEN 'mid' ELSE 'low' END
ORDER BY shipping_fee_bucket ASC;
