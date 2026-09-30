SELECT
    id,
    channel,
    promo_code,
    LAG(discount_pct) OVER w_recent AS prev_discount_pct,
    LEAD(discount_pct) OVER w_recent AS next_discount_pct,
    ROW_NUMBER() OVER w_recent AS seq_no,
    ROW_NUMBER() OVER (PARTITION BY channel ORDER BY ends_on, id) AS seq_no_2
FROM promotions
WINDOW w_recent AS (PARTITION BY channel ORDER BY starts_on, id);
