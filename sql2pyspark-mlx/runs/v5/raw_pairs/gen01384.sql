WITH valid AS (
    SELECT
        id,
        method,
        paid_at,
        status,
        TO_DATE(paid_at) AS paid_at_date,
        CAST(amount AS INT) AS amount_int
    FROM payments
    WHERE paid_at > TIMESTAMP '2023-04-28 11:00:00'
      AND method IS NOT NULL
)
SELECT
    id,
    method,
    status,
    paid_at_date,
    amount_int,
    FIRST_VALUE(method, TRUE) OVER (PARTITION BY method ORDER BY paid_at_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_method,
    CUME_DIST() OVER (ORDER BY amount_int ASC) AS amount_int_cume_dist,
    FIRST_VALUE(method) IGNORE NULLS OVER (PARTITION BY status, method ORDER BY amount_int ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_method_2,
    ROW_NUMBER() OVER (PARTITION BY method, status ORDER BY paid_at DESC, id ASC) AS row_seq
FROM valid
