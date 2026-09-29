SELECT id, channel, amount, discount, NTILE(3) OVER (PARTITION BY channel ORDER BY discount, id) AS discount_bucket, LEAD(discount, 1) OVER (ORDER BY amount, id) AS next_discount FROM orders;
