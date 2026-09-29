SELECT
    *,
    DENSE_RANK() OVER w_ordered AS amount_dense_rank,
    ROW_NUMBER() OVER w_recent AS seq_no,
    LAST_VALUE(method, TRUE) OVER w_hist AS last_known_method
FROM payments
WINDOW w_ordered AS (ORDER BY amount), w_recent AS (ORDER BY amount, id), w_hist AS (PARTITION BY method ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
