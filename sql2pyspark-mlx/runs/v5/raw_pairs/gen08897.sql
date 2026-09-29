SELECT *, ROW_NUMBER() OVER (PARTITION BY promo_code, channel ORDER BY discount_pct, id) AS rank_in_group FROM promotions QUALIFY rank_in_group = 1;
