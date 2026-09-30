SELECT channel, SUM(shipping_fee) AS total_shipping_fee, DENSE_RANK() OVER (ORDER BY SUM(shipping_fee) DESC) AS rnk FROM orders GROUP BY channel;
