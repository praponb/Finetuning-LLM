WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        item.promo
    FROM carts
    LATERAL VIEW OUTER EXPLODE(items) x AS item
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id
    FROM flattened
    EXCEPT ALL
    SELECT id
    FROM carts
    WHERE ARRAY_CONTAINS(coupon_codes, 'WELCOME5')
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM flattened s
    INNER JOIN remaining_keys k ON s.id = k.id
),
kept_keys AS (
    -- Set intersection on keys
    SELECT customer_id
    FROM reconciled
    INTERSECT ALL
    SELECT customer_id
    FROM carts
    WHERE ARRAY_CONTAINS(coupon_codes, 'SPRING10')
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM reconciled s
    INNER JOIN kept_keys k ON s.customer_id = k.customer_id
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        e.customer_id,
        e.promo,
        c.credit_limit,
        c.segment
    FROM kept_rows e
    LEFT JOIN customers c
        ON e.customer_id <=> c.id
),
joined AS (
    -- Attach customers attributes
    SELECT a.*, c.segment AS customer_segment
    FROM with_dims a
    INNER JOIN customers c
        ON a.customer_id = c.id
)
-- Join a dimension and aggregate
SELECT
    dim.segment AS customer_segment,
    src.promo,
    COUNT(*) AS num_rows,
    MAX(src.credit_limit) AS max_credit_limit
FROM joined src
INNER JOIN customers dim ON src.customer_id = dim.id
GROUP BY CUBE (dim.segment, src.promo);
