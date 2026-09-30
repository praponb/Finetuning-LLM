WITH valid AS (
    SELECT
        *,
        CASE WHEN amount >= 1660 THEN 'High' WHEN amount >= 510 THEN 'Medium' ELSE 'Low' END AS amount_band,
        COALESCE(method, 'NONE') AS method_clean
    FROM payments
    WHERE paid_at > TIMESTAMP '2023-08-10 05:00:00'
      AND order_id IS NOT NULL
),
filtered AS (
    SELECT
        id,
        order_id,
        method,
        amount,
        paid_at,
        status,
        amount_band,
        method_clean,
        LENGTH(status) AS status_length,
        TRIM(status) AS clean_status
    FROM valid
    WHERE paid_at >= TO_TIMESTAMP('2023-05-19 19:00:00')
),
scored AS (
    SELECT
        id,
        order_id,
        method,
        status_length,
        FIRST_VALUE(amount, TRUE) OVER (ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_amount,
        ROW_NUMBER() OVER (PARTITION BY order_id, status ORDER BY status_length DESC, id) AS seq_no,
        DENSE_RANK() OVER (PARTITION BY order_id ORDER BY status_length DESC NULLS LAST) AS status_length_dense_rank,
        LAG(status_length, 2) OVER (PARTITION BY method ORDER BY paid_at, id) AS prev_status_length
    FROM filtered
),
joined AS (
    SELECT
        e.id,
        e.order_id,
        e.method,
        e.status_length,
        e.first_known_amount,
        e.seq_no,
        e.status_length_dense_rank,
        e.prev_status_length,
        p.discount,
        p.amount,
        p.status
    FROM scored e
    LEFT JOIN orders p
        ON e.order_id = p.id
)
SELECT
    id,
    status,
    metric,
    val
FROM (
    SELECT
        id,
        status,
        discount,
        amount,
        first_known_amount
    FROM joined
) src
UNPIVOT INCLUDE NULLS (
    val FOR metric IN (discount, amount, first_known_amount)
)
