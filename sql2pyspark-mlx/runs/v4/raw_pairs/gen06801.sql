WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        elem.qty,
        elem.price
    FROM carts
    LATERAL VIEW EXPLODE(items) x AS elem
),
kept_keys AS (
    -- Set difference on keys
    SELECT customer_id, id
    FROM exploded
    EXCEPT
    SELECT customer_id, id
    FROM carts
    WHERE HOUR(created_at) BETWEEN 8 AND 12
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM exploded s
    INNER JOIN kept_keys k ON s.customer_id = k.customer_id AND s.id = k.id
),
joined AS (
    -- Attach customers attributes
    SELECT a.*, b.credit_limit
    FROM verified a
    LEFT JOIN customers b
        ON a.customer_id = b.id
)
-- Multi-level aggregation
SELECT
    CASE WHEN qty >= 5 THEN 'LARGE' ELSE 'SMALL' END AS qty_tier,
    PERCENTILE_APPROX(credit_limit, 0.95) AS credit_limit_p95,
    ROUND(AVG(qty), 2) AS avg_qty
FROM joined
GROUP BY CASE WHEN qty >= 5 THEN 'LARGE' ELSE 'SMALL' END
HAVING avg_qty >= 0
ORDER BY qty_tier DESC NULLS LAST;
