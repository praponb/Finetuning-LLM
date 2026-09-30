SELECT
    id,
    amount,
    paid_at,
    LEAD(amount) OVER w_grp AS next_amount,
    ROW_NUMBER() OVER w_grp AS seq_no
FROM payments
WINDOW w_grp AS (PARTITION BY order_id, method ORDER BY amount, id)
