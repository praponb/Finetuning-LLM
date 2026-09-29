WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        k,
        value
    FROM carts
    LATERAL VIEW EXPLODE(attributes) t AS k, value
),
flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        k,
        element
    FROM lines
    LATERAL VIEW EXPLODE(coupon_codes) e AS element
),
matched_keys AS (
    -- Set intersection on keys
    SELECT id, customer_id
    FROM flattened
    INTERSECT
    SELECT id, customer_id
    FROM carts
    WHERE customer_id IN (19, 38)
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM flattened s
    INNER JOIN matched_keys k ON s.id = k.id AND s.customer_id = k.customer_id
)
SELECT
    s.customer_id,
    s.created_at,
    s.k,
    s.element,
    CASE WHEN element RLIKE '^(VIP|BUNDLE)$' THEN 'CORE' ELSE 'GROUP_A' END AS element_zone,
    (SELECT MIN(a2.balance) FROM accounts a2 WHERE a2.customer_id = s.customer_id) AS min_account_balance
FROM kept_rows s
ORDER BY customer_id NULLS FIRST;
