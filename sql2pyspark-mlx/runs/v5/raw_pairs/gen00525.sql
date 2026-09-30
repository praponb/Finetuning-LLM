WITH cleaned AS (
    SELECT
        *,
        CASE WHEN amount >= 1340 THEN 'High' WHEN amount >= 640 THEN 'Medium' ELSE 'Low' END amount_tier,
        COALESCE(channel, 'N/A') channel_clean
    FROM orders
    WHERE customer_id IS NOT NULL
      AND channel = 'partner'
),
scored AS (
    SELECT
        id,
        customer_id,
        status,
        amount,
        LEAD(discount) OVER w_grp next_discount,
        LAG(amount) OVER w_grp prev_amount,
        LEAD(amount) OVER w_grp next_amount,
        ROW_NUMBER() OVER w_grp row_seq
    FROM cleaned
    WINDOW w_grp AS (PARTITION BY customer_id ORDER BY amount, id)
),
eligible AS (
    SELECT s.*
    FROM scored s
    WHERE EXISTS (
            SELECT 1
            FROM payments x
            WHERE x.order_id = s.id
              AND x.status IN ('SUCCESS', 'FAILED')
        )
),
ranked AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id, status ORDER BY next_discount DESC, id) AS dedup_rank
        FROM eligible
    ) x
    WHERE dedup_rank = 1
)
SELECT
    id,
    status,
    amount,
    next_discount,
    prev_amount,
    row_seq,
    LEAD(next_amount) OVER (PARTITION BY status, customer_id ORDER BY row_seq, id) next_next_amount,
    LAG(next_discount) OVER (PARTITION BY status, customer_id ORDER BY row_seq, id) prev_next_discount,
    ROW_NUMBER() OVER (PARTITION BY status, customer_id ORDER BY row_seq, id) seq_no
FROM ranked
WHERE dedup_rank <= 1;
