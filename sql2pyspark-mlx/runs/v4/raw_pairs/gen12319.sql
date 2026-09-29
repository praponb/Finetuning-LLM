SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY channel ORDER BY ends_on, id) AS seq_no,
    ROUND(discount_pct / SUM(discount_pct) OVER (PARTITION BY promo_code, channel), 4) AS discount_pct_share
FROM promotions
