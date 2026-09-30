SELECT channel, SUM(shipping_fee) AS total_shipping_fee, COUNT(*) AS cnt, DENSE_RANK() OVER (ORDER BY SUM(shipping_fee) DESC) AS rnk FROM orders GROUP BY channel ORDER BY cnt DESC;
