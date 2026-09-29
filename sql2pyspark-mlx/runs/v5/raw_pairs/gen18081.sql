WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        value
    FROM carts
    LATERAL VIEW EXPLODE(coupon_codes) t AS value
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        COALESCE(value, 'Unknown') AS value_filled,
        CASE WHEN value IN ('SPRING10', 'VIP') THEN 'Group A' ELSE 'Other' END AS value_group,
        COALESCE(value, 'DIRECT') AS resolved_value
    FROM exploded
    WHERE customer_id IS NOT NULL
      AND value IS NOT NULL
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        LOWER(value) AS value_lower,
        COALESCE(value, 'DIRECT') AS value_filled_2
    FROM valid
    WHERE customer_id IS NOT NULL
      AND value IS NOT DISTINCT FROM 'SPRING10'
),
kept_keys AS (
    -- Set difference on keys
    SELECT customer_id
    FROM cleaned
    EXCEPT ALL
    SELECT customer_id
    FROM carts
    WHERE HOUR(created_at) BETWEEN 11 AND 19
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM cleaned s
    INNER JOIN kept_keys k ON s.customer_id = k.customer_id
),
joined AS (
    -- Attach customers attributes
    SELECT
        src.*,
        b.signup_date,
        b.credit_limit,
        b.age
    FROM reconciled src
    LEFT JOIN customers b
        ON src.customer_id <=> b.id
)
SELECT
    customer_id,
    value_filled,
    value_group,
    value_lower,
    credit_limit,
    age
FROM joined
ORDER BY customer_id;
