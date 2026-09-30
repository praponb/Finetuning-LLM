WITH lines AS (
    SELECT
        id,
        customer_id,
        created_at,
        tag
    FROM carts
    LATERAL VIEW OUTER EXPLODE(coupon_codes) e AS tag
),
remaining_keys AS (
    SELECT id
    FROM lines
    EXCEPT
    SELECT id
    FROM carts
    WHERE ARRAY_CONTAINS(coupon_codes, 'WELCOME5')
),
reconciled AS (
    SELECT s.*
    FROM lines s
    INNER JOIN remaining_keys k ON s.id = k.id
),
enriched AS (
    SELECT
        src.customer_id,
        src.created_at,
        p.age
    FROM reconciled src
    LEFT JOIN customers p
        ON src.customer_id = p.id
),
joined AS (
    SELECT
        s.customer_id,
        s.created_at,
        s.age,
        d.country,
        d.age AS customer_age,
        d.name
    FROM enriched s
    LEFT JOIN customers d
        ON s.customer_id <=> d.id
)
SELECT
    customer_id,
    created_at,
    age,
    country,
    name
FROM joined
ORDER BY country ASC NULLS FIRST;
