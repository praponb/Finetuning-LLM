WITH flattened AS (
    SELECT
        id,
        customer_id,
        created_at,
        k,
        attr_value
    FROM carts
    LATERAL VIEW EXPLODE(attributes) t AS k, attr_value
),
enriched AS (
    SELECT
        e.*,
        d.age,
        d.city,
        d.email
    FROM flattened e
    LEFT JOIN customers d
        ON e.customer_id = d.id
),
cleaned AS (
    SELECT *, ROUND(age * 0.9, 2) AS age_adjusted
    FROM enriched
    WHERE city IS NOT NULL
      AND age <= 46
),
kept_keys AS (
    SELECT customer_id
    FROM cleaned
    INTERSECT
    SELECT customer_id
    FROM carts
    WHERE customer_id IS NOT NULL
),
verified AS (
    SELECT s.*
    FROM cleaned s
    INNER JOIN kept_keys k ON s.customer_id = k.customer_id
),
matched_keys AS (
    SELECT id, customer_id
    FROM verified
    INTERSECT
    SELECT id, customer_id
    FROM carts
    WHERE HOUR(created_at) BETWEEN 10 AND 15
),
reconciled AS (
    SELECT s.*
    FROM verified s
    INNER JOIN matched_keys k ON s.id = k.id AND s.customer_id = k.customer_id
)
SELECT *
FROM (
    SELECT
        customer_id,
        k,
        city,
        age
    FROM reconciled
) src
PIVOT (
    SUM(age) AS value_sum,
    COUNT(1) AS n
    FOR city IN ('Chiang Mai' AS chiang_mai, 'Singapore' AS singapore, 'London' AS london)
)
