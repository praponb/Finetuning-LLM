SELECT
    *,
    PERCENT_RANK() OVER (ORDER BY discount_pct) AS discount_pct_percent_rank,
    ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY ends_on, id) AS row_seq,
    LAG(discount_pct, 1) OVER (ORDER BY ends_on, id) AS prev_discount_pct,
    AVG(discount_pct) OVER (PARTITION BY promo_code, channel ORDER BY ends_on, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS discount_pct_avg_5_prior
FROM promotions;
