SELECT *, ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY discount_pct DESC, id) AS rank_in_group FROM promotions QUALIFY rank_in_group = 1;
