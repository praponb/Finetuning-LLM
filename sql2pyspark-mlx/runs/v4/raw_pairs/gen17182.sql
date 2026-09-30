WITH with_windows AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY account_type, status ORDER BY opened_date, id) AS event_seq,
        ROW_NUMBER() OVER (PARTITION BY customer_id, status ORDER BY balance DESC, id) AS seq_no
    FROM accounts
)
SELECT
    s.id,
    s.customer_id,
    s.status,
    s.event_seq,
    s.seq_no,
    (SELECT SUM(x.discount) FROM orders x WHERE x.customer_id = s.customer_id) AS sum_order_discount
FROM with_windows s;
