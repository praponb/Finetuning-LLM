WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        paid_at,
        status,
        CONCAT_WS('-', status, method) status_method_key,
        CONCAT_WS('-', status, method) status_method_key_2,
        COALESCE(method, 'UNKNOWN') resolved_method
    FROM payments
    WHERE order_id IS NOT NULL
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY paid_at DESC, id ASC) event_seq,
        ROW_NUMBER() OVER (ORDER BY paid_at DESC, id ASC) seq_no,
        ROW_NUMBER() OVER (ORDER BY paid_at ASC, id ASC) seq_no_2,
        ROW_NUMBER() OVER (ORDER BY paid_at DESC, id ASC) row_seq,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY paid_at DESC, id ASC) row_seq_2
    FROM base
),
joined AS (
    -- Attach orders attributes
    SELECT
        a.id,
        a.order_id,
        a.status_method_key,
        a.status_method_key_2,
        a.seq_no_2,
        a.row_seq,
        a.row_seq_2,
        b.channel,
        b.order_date,
        b.discount
    FROM metrics a
    LEFT JOIN orders b
        ON a.order_id = b.id
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY row_seq_2 DESC NULLS LAST, id ASC) AS rn
        FROM joined
    ) ranked
    WHERE rn = 1
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        order_id,
        status_method_key_2,
        row_seq,
        row_seq_2,
        channel,
        order_date,
        discount,
        LAG(row_seq_2) OVER w_recent prev_row_seq_2,
        LEAD(row_seq_2) OVER w_recent next_row_seq_2,
        LAG(discount) OVER w_recent prev_discount,
        ROW_NUMBER() OVER w_recent seq_no,
        SUM(row_seq_2) OVER (ORDER BY rn ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_row_seq_2
    FROM deduped
    WINDOW w_recent AS (PARTITION BY channel, order_id ORDER BY order_date ASC, id ASC)
)
-- Running and moving aggregates
SELECT
    id,
    status_method_key_2,
    seq_no,
    running_row_seq_2,
    RANK() OVER (PARTITION BY order_id ORDER BY row_seq DESC NULLS LAST) row_seq_rank,
    FIRST_VALUE(seq_no) IGNORE NULLS OVER (ORDER BY running_row_seq_2 ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_seq_no
FROM scored
WHERE row_seq <= 7;
