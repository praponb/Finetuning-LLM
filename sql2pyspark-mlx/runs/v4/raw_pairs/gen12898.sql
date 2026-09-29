WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        k,
        attr_value
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) t AS k, attr_value
),
joined AS (
    -- Attach customers attributes
    SELECT e.*, d.country
    FROM lines e
    LEFT JOIN customers d
        ON e.customer_id = d.id
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, LOWER(k) AS k_lower
    FROM joined
    WHERE country IS NOT NULL
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        LOWER(k) AS k_lower_2,
        TRIM(k) AS clean_k,
        COALESCE(k, 'DIRECT') AS k_clean
    FROM cleaned
    WHERE customer_id = 22
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT *, CONCAT(country, ' - ', k) AS country_k
    FROM valid
    WHERE customer_id IS NULL
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        a.*,
        p.age,
        p.credit_limit,
        p.name
    FROM filtered a
    LEFT JOIN customers p
        ON a.customer_id = p.id
),
matched_keys AS (
    -- Set difference on keys
    SELECT customer_id
    FROM with_dims
    EXCEPT DISTINCT
    SELECT customer_id
    FROM carts
    WHERE ARRAY_CONTAINS(coupon_codes, 'SPRING10')
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_dims s
    INNER JOIN matched_keys k ON s.customer_id = k.customer_id
),
pivoted AS (
    -- Native SQL PIVOT
    SELECT *
    FROM (
        SELECT
            customer_id,
            k,
            credit_limit
        FROM reconciled
    ) src
    PIVOT (
        MAX(credit_limit)
        FOR k IN ('device', 'gift')
    )
)
SELECT
    s.customer_id,
    s.device,
    s.gift,
    (SELECT AVG(o2.shipping_fee) FROM orders o2 WHERE o2.customer_id = s.customer_id) AS avg_order_shipping_fee
FROM pivoted s
ORDER BY customer_id ASC;
