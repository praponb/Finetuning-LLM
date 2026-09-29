WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        DATE_TRUNC('month', paid_at) paid_at_period,
        COALESCE(method, 'N/A') resolved_method
    FROM payments
    WHERE paid_at > TIMESTAMP '2023-09-01 04:00:00'
      AND order_id IS NOT NULL
      AND status IN ('SUCCESS', 'FAILED')
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY paid_at_period DESC, id ASC) event_seq,
        ROW_NUMBER() OVER w_hist row_seq,
        ROW_NUMBER() OVER w_hist event_seq_2,
        ROW_NUMBER() OVER w_recent seq_no,
        ROW_NUMBER() OVER w_hist_2 seq_no_2
    FROM base
    WINDOW w_hist AS (PARTITION BY order_id ORDER BY paid_at_period ASC, id ASC), w_recent AS (PARTITION BY order_id ORDER BY paid_at_period DESC, id ASC), w_hist_2 AS (ORDER BY paid_at_period ASC, id ASC)
),
scored_payments AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        order_id,
        paid_at_period,
        resolved_method,
        event_seq,
        row_seq,
        event_seq_2,
        seq_no,
        seq_no_2,
        SUM(seq_no_2) OVER (ORDER BY seq_no_2 ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_seq_no_2,
        ROUND(seq_no / SUM(seq_no) OVER (PARTITION BY order_id), 4) seq_no_share,
        NTILE(3) OVER (PARTITION BY order_id ORDER BY event_seq_2 ASC, id ASC) event_seq_2_bucket
    FROM scored
)
SELECT
    s.id,
    s.order_id,
    s.paid_at_period,
    s.resolved_method,
    s.row_seq,
    s.event_seq_2,
    s.seq_no,
    s.seq_no_2,
    s.cumulative_seq_no_2,
    (SELECT MAX(t2.amount) FROM payments t2 WHERE t2.order_id = s.order_id) AS max_payment_amount
FROM scored_payments s
ORDER BY paid_at_period DESC;
