SELECT channel, ROUND(STDDEV(shipping_fee), 2) AS stddev_shipping_fee FROM orders GROUP BY channel;
