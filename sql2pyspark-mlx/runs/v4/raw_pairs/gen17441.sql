SELECT *, ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY discount_pct DESC, id) AS dedup_rank
FROM promotions
QUALIFY dedup_rank = 1
