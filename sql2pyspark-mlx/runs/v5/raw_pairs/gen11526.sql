WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        k,
        value
    FROM carts
    LATERAL VIEW EXPLODE(attributes) li AS k, value
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        a.*,
        c.credit_limit,
        c.age
    FROM lines a
    LEFT JOIN customers c
        ON a.customer_id = c.id
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        customer_id,
        k,
        value,
        credit_limit,
        age,
        FIRST_VALUE(credit_limit) IGNORE NULLS OVER (ORDER BY credit_limit, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_credit_limit,
        RANK() OVER (PARTITION BY k, customer_id ORDER BY credit_limit DESC NULLS LAST) AS credit_limit_rank,
        FIRST_VALUE(credit_limit, TRUE) OVER (PARTITION BY k ORDER BY credit_limit, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_credit_limit_2,
        ROW_NUMBER() OVER w_recent AS row_seq
    FROM with_dims
    WINDOW w_recent AS (PARTITION BY k ORDER BY credit_limit DESC, id, k)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        MIN(credit_limit) OVER (PARTITION BY customer_id ORDER BY credit_limit, id, k ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_min_credit_limit,
        COUNT(credit_limit) OVER (PARTITION BY customer_id ORDER BY credit_limit, id, k ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_count_credit_limit,
        SUM(credit_limit) OVER (PARTITION BY customer_id ORDER BY credit_limit, id, k ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_sum_credit_limit,
        LAST_VALUE(first_known_credit_limit, TRUE) OVER (ORDER BY row_seq, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_first_known_credit_limit
    FROM metrics
    WHERE row_seq BETWEEN 3 AND 7
)
-- Join a dimension and aggregate
SELECT
    p.city,
    src.k,
    COUNT(*) AS cnt,
    MAX(src.age) AS max_age
FROM with_windows src
JOIN customers p ON src.customer_id = p.id
GROUP BY p.city, src.k
