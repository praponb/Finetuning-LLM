WITH ranked AS (SELECT * FROM promotions QUALIFY RANK() OVER (PARTITION BY promo_code ORDER BY discount_pct) = 1) SELECT id, channel, ends_on FROM ranked;
