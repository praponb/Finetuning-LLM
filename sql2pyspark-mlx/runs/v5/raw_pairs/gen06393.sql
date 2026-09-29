SELECT channel, MIN(discount) AS min_discount, ROUND(AVG(shipping_fee), 2) AS avg_shipping_fee FROM orders WHERE shipping_fee <= 16 GROUP BY channel;
