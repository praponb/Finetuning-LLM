WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        EXPLODE_OUTER(coupon_codes) AS tag
    FROM carts
),
base AS (
    -- Keep recent, valid rows
    SELECT *, LOWER(tag) AS tag_lower
    FROM flattened
    WHERE tag <> 'VIP'
),
joined AS (
    -- Attach customers attributes
    SELECT
        src.id,
        src.customer_id,
        p.name
    FROM base src
    INNER JOIN customers p
        ON src.customer_id = p.id
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        e.*,
        d.city,
        d.credit_limit
    FROM joined e
    INNER JOIN customers d
        ON e.customer_id = d.id
    WHERE d.credit_limit <= 15300
),
remaining_keys AS (
    -- Set difference on keys
    SELECT customer_id, id
    FROM with_dims
    EXCEPT
    SELECT customer_id, id
    FROM carts
    WHERE customer_id IS NOT NULL
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_dims s
    INNER JOIN remaining_keys k ON s.customer_id = k.customer_id AND s.id = k.id
)
SELECT
    s.id,
    s.customer_id,
    s.city,
    s.credit_limit,
    CASE WHEN city RLIKE '^(Singapore|Bangkok)$' THEN 'PRIORITY' ELSE 'EXTENDED' END AS city_zone,
    (SELECT SUM(s2.monthly_fee) FROM subscriptions s2 WHERE s2.customer_id = s.customer_id) AS sum_subscription_monthly_fee
FROM reconciled s
ORDER BY customer_id;
