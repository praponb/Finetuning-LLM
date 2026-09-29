WITH lines AS (
    SELECT
        id,
        customer_id,
        key,
        value
    FROM carts
    LATERAL VIEW EXPLODE(attributes) x AS key, value
),
enriched AS (
    SELECT
        e.*,
        d.country,
        d.email
    FROM lines e
    INNER JOIN customers d
        ON e.customer_id = d.id
),
with_dims AS (
    SELECT
        e.*,
        c.name,
        c.country AS customer_country
    FROM enriched e
    LEFT JOIN customers c
        ON e.customer_id = c.id
)
SELECT
    id,
    customer_id,
    key,
    value,
    country
FROM with_dims
ORDER BY customer_id DESC NULLS FIRST
