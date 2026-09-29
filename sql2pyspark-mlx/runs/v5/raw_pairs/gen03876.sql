WITH exploded AS (
    SELECT
        c.id,
        c.customer_id,
        c.created_at,
        c.coupon_codes,
        item.product_id,
        item.price
    FROM carts c
    LATERAL VIEW EXPLODE(c.items) t AS item
),
enriched AS (
    SELECT
        a.id,
        a.customer_id,
        b.name,
        b.signup_date,
        b.country
    FROM exploded a
    LEFT JOIN customers b
        ON a.customer_id <=> b.id
    WHERE b.age BETWEEN 53 AND 64
),
cleaned AS (
    SELECT
        id,
        name,
        signup_date,
        COALESCE(name, 'unassigned') AS name_filled
    FROM enriched
    WHERE customer_id IS NOT NULL
),
remaining_keys AS (
    SELECT id
    FROM cleaned
    EXCEPT
    SELECT id
    FROM carts
    WHERE ARRAY_CONTAINS(coupon_codes, 'VIP')
),
reconciled AS (
    SELECT s.*
    FROM cleaned s
    JOIN remaining_keys k ON s.id = k.id
)
SELECT
    DATE_TRUNC('QUARTER', signup_date) AS quarter_start,
    COUNT(*) AS cnt,
    COUNT(*) AS row_count,
    COUNT(*) AS num_rows,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(*) AS row_count_2,
    GROUPING_ID() AS gid
FROM reconciled
GROUP BY CUBE (DATE_TRUNC('QUARTER', signup_date));
