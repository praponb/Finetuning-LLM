SELECT id, channel, discount, RANK() OVER (PARTITION BY channel ORDER BY discount DESC) AS rnk FROM orders;
