SELECT
    id,
    order_id,
    amount,
    NTILE(3) OVER (PARTITION BY order_id ORDER BY amount ASC, id ASC) AS amount_bucket,
    CUME_DIST() OVER (PARTITION BY status ORDER BY amount ASC) AS amount_cume_dist
FROM payments;
