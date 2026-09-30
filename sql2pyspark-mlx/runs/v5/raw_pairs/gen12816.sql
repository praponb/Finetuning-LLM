WITH exploded AS (
    SELECT
        id,
        customer_id,
        created_at,
        k,
        value
    FROM carts
    LATERAL VIEW EXPLODE(attributes) x AS k, value
)
SELECT
    id,
    customer_id,
    k,
    ROW_NUMBER() OVER (PARTITION BY k ORDER BY created_at, id, k) AS seq_no,
    LAST_VALUE(k, TRUE) OVER (PARTITION BY customer_id ORDER BY created_at, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_k,
    FIRST_VALUE(k, TRUE) OVER (PARTITION BY customer_id ORDER BY created_at, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_k,
    LAST_VALUE(k, TRUE) OVER (PARTITION BY k ORDER BY created_at, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_k_2,
    FIRST_VALUE(k, TRUE) OVER (PARTITION BY customer_id ORDER BY created_at, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_k_2
FROM exploded
