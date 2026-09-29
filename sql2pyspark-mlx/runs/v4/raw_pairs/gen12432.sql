WITH base AS (
    SELECT *, CAST(amount AS INT) AS amount_int
    FROM orders
    WHERE store_id IS NOT NULL
      AND status = 'COMPLETED'
),
metrics AS (
    SELECT
        *,
        ROW_NUMBER() OVER w_part AS event_seq,
        LAG(amount, 2) OVER (PARTITION BY customer_id ORDER BY shipping_fee, id) AS prev_amount
    FROM base
    WINDOW w_part AS (PARTITION BY channel ORDER BY order_date, id)
),
matched_keys AS (
    SELECT status, customer_id
    FROM metrics
    EXCEPT
    SELECT status, customer_id
    FROM orders
    WHERE store_id IS NOT NULL
),
kept_rows AS (
    SELECT s.*
    FROM metrics s
    JOIN matched_keys k ON s.status = k.status AND s.customer_id = k.customer_id
)
SELECT
    *,
    CORR(discount, amount) OVER (PARTITION BY channel) AS corr_discount_amount,
    CUME_DIST() OVER w_grp AS prev_amount_cume_dist,
    PERCENT_RANK() OVER w_hist AS event_seq_percent_rank,
    DENSE_RANK() OVER w_part AS event_seq_dense_rank,
    ROUND(amount / SUM(amount) OVER (PARTITION BY store_id, channel), 4) AS amount_share
FROM kept_rows
WINDOW w_grp AS (PARTITION BY store_id ORDER BY prev_amount NULLS LAST), w_hist AS (PARTITION BY store_id, customer_id ORDER BY event_seq NULLS FIRST), w_part AS (ORDER BY event_seq NULLS FIRST)
