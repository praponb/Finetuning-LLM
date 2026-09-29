WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        coupon_codes,
        k,
        attr_value
    FROM carts
    LATERAL VIEW EXPLODE(attributes) e AS k, attr_value
),
lines_carts AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        k,
        attr_value,
        EXPLODE_OUTER(coupon_codes) AS tag
    FROM lines
),
joined AS (
    -- Attach customers attributes
    SELECT
        src.*,
        p.country,
        p.credit_limit,
        p.city
    FROM lines_carts src
    JOIN customers p
        ON src.customer_id = p.id
    WHERE p.segment = 'SMB'
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        k,
        tag,
        country,
        credit_limit,
        city,
        CAST(credit_limit AS INT) credit_limit_int
    FROM joined
    WHERE city IS NOT NULL
      AND credit_limit BETWEEN 5000 AND 10100
)
-- Join a dimension and aggregate
SELECT
    d.city AS customer_city,
    d.segment,
    e.tag,
    COUNT(*) AS num_rows,
    ROUND(SUM(e.credit_limit), 2) AS total_credit_limit
FROM valid e
JOIN customers d ON e.customer_id = d.id
GROUP BY ROLLUP (d.city, d.segment, e.tag);
