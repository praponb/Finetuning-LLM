WITH lines AS (
    SELECT
        id,
        customer_id,
        created_at,
        element
    FROM carts
    LATERAL VIEW EXPLODE(coupon_codes) x AS element
),
enriched AS (
    SELECT s.*, c.age
    FROM lines s
    INNER JOIN customers c
        ON s.customer_id = c.id
),
enriched_carts AS (
    SELECT
        e.*,
        p.country,
        p.segment,
        p.age AS customer_age
    FROM enriched e
    LEFT JOIN customers p
        ON e.customer_id = p.id
),
totals AS (
    SELECT
        CASE WHEN age >= 58 THEN 'HIGH' ELSE 'LOW' END AS age_tier,
        segment,
        COUNT(DISTINCT id) AS distinct_ids,
        (UNIX_TIMESTAMP(MAX(created_at)) - UNIX_TIMESTAMP(MIN(created_at))) / 60 AS duration_min,
        COUNT(CASE WHEN country = 'SG' THEN 1 END) AS sg_count,
        GROUPING_ID() AS gid
    FROM enriched_carts
    GROUP BY CUBE (CASE WHEN age >= 58 THEN 'HIGH' ELSE 'LOW' END, segment)
)
SELECT
    age_tier,
    segment,
    distinct_ids,
    duration_min,
    sg_count,
    gid
FROM totals
