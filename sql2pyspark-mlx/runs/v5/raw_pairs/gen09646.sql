WITH deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY order_id ORDER BY amount DESC NULLS FIRST) AS row_num
        FROM payments
    ) x
    WHERE row_num = 1
),
valid AS (
    -- Keep recent, valid rows
    SELECT *, HOUR(paid_at) AS paid_at_hour
    FROM deduped
    WHERE paid_at > TO_DATE('2023-08-28')
),
enriched AS (
    -- Attach orders attributes
    SELECT
        src.*,
        b.order_date,
        b.channel
    FROM valid src
    LEFT JOIN orders b
        ON src.order_id = b.id
),
enriched_payments AS (
    -- Ranking and gap features
    SELECT
        *,
        MIN(paid_at_hour) OVER (PARTITION BY order_id) AS order_id_min_paid_at_hour,
        DENSE_RANK() OVER (PARTITION BY method ORDER BY paid_at_hour DESC) AS paid_at_hour_dense_rank,
        SUM(paid_at_hour) OVER w_recent AS paid_at_hour_sum_7d
    FROM enriched
    WHERE paid_at_hour = 36
    WINDOW w_recent AS (PARTITION BY status, channel ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW)
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        method,
        status,
        amount
    FROM enriched_payments
) src
PIVOT (
    SUM(amount) AS amount
    FOR status IN ('SUCCESS', 'REFUNDED', 'FAILED')
);
