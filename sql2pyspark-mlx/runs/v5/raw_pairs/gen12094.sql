SELECT
    *,
    PERCENT_RANK() OVER w_grp AS discount_pct_percent_rank,
    SUM(discount_pct) OVER w_running AS running_discount_pct,
    LEAD(discount_pct, 1) OVER (PARTITION BY channel ORDER BY ends_on, id) AS next_discount_pct,
    ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY ends_on DESC, id) AS event_seq
FROM promotions
WINDOW w_grp AS (ORDER BY discount_pct), w_running AS (PARTITION BY promo_code ORDER BY starts_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
