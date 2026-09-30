SELECT
    id,
    customer_id,
    store_id,
    order_date,
    status,
    amount,
    shipping_fee,
    AVG(discount) OVER (PARTITION BY status ORDER BY amount ASC, id ASC ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS discount_avg_6_prior,
    SUM(discount) OVER w_grp AS running_discount,
    ROW_NUMBER() OVER (PARTITION BY channel ORDER BY discount DESC, id ASC) AS seq_no
FROM orders
WINDOW w_grp AS (PARTITION BY customer_id, channel ORDER BY discount ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW);
