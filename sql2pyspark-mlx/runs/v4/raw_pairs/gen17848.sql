SELECT
    *,
    CORR(discount, amount) OVER w_part AS corr_discount_amount,
    CUME_DIST() OVER w_hist AS discount_cume_dist
FROM orders
WINDOW w_part AS (PARTITION BY customer_id), w_hist AS (PARTITION BY customer_id ORDER BY discount DESC)
