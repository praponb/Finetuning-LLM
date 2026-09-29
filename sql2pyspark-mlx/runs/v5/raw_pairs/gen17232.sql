WITH base AS (
    SELECT
        id,
        method,
        status,
        CAST(paid_at AS DATE) AS paid_at_day
    FROM payments
    WHERE paid_at > TO_DATE('2023-08-23')
      AND order_id IS NOT NULL
),
enriched AS (
    SELECT
        id,
        paid_at_day,
        FIRST_VALUE(method, TRUE) OVER (ORDER BY paid_at_day, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_method,
        ROW_NUMBER() OVER (PARTITION BY method ORDER BY paid_at_day, id) AS seq_no,
        ROW_NUMBER() OVER (ORDER BY paid_at_day, id) AS row_seq
    FROM base
),
enriched_payments AS (
    SELECT
        *,
        LEAD(row_seq, 2) OVER (ORDER BY seq_no, id) AS next_row_seq,
        PERCENT_RANK() OVER w_recent AS row_seq_percent_rank,
        ROW_NUMBER() OVER w_part AS row_seq_2
    FROM enriched
    WHERE YEAR(paid_at_day) >= 2025
    WINDOW w_recent AS (ORDER BY row_seq NULLS FIRST), w_part AS (ORDER BY paid_at_day DESC, id)
),
scored AS (
    SELECT
        id,
        paid_at_day,
        first_known_method,
        row_seq_percent_rank,
        row_seq_2,
        FIRST_VALUE(seq_no, TRUE) OVER (ORDER BY seq_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_seq_no,
        PERCENT_RANK() OVER (ORDER BY row_seq_percent_rank DESC) AS row_seq_percent_rank_percent_rank
    FROM enriched_payments
)
SELECT
    id,
    paid_at_day,
    row_seq_percent_rank,
    row_seq_2,
    row_seq_percent_rank_percent_rank
FROM scored
