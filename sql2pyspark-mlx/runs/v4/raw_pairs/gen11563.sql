WITH lines AS (
    SELECT
        id,
        customer_id,
        EXPLODE(coupon_codes) AS element
    FROM carts
),
joined AS (
    SELECT
        src.*,
        p.credit_limit,
        p.country,
        p.city
    FROM lines src
    LEFT JOIN customers p
        ON src.customer_id = p.id
),
with_windows AS (
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY element ORDER BY credit_limit DESC NULLS LAST) AS credit_limit_cume_dist,
        DENSE_RANK() OVER (ORDER BY credit_limit DESC NULLS FIRST) AS credit_limit_dense_rank
    FROM joined
),
enriched AS (
    SELECT
        src.*,
        p.credit_limit AS customer_credit_limit,
        p.email,
        p.name
    FROM with_windows src
    LEFT JOIN customers p
        ON src.customer_id = p.id
),
remaining_keys AS (
    SELECT customer_id, id
    FROM enriched
    INTERSECT
    SELECT customer_id, id
    FROM carts
    WHERE SIZE(coupon_codes) >= 2
),
verified AS (
    SELECT s.*
    FROM enriched s
    INNER JOIN remaining_keys k ON s.customer_id = k.customer_id AND s.id = k.id
),
enriched_carts AS (
    SELECT
        s.*,
        b.name AS customer_name,
        b.city AS customer_city,
        b.segment
    FROM verified s
    LEFT JOIN customers b
        ON s.customer_id = b.id
),
enriched_carts_2 AS (
    SELECT e.*, p.age
    FROM enriched_carts e
    LEFT JOIN customers p
        ON e.customer_id = p.id
),
grouped AS (
    SELECT
        element,
        CASE WHEN credit_limit_dense_rank >= 9 THEN 'PREMIUM' ELSE 'STANDARD' END AS credit_limit_dense_rank_tier,
        MIN(credit_limit_dense_rank) AS min_credit_limit_dense_rank,
        SIZE(COLLECT_SET(country)) AS num_countries,
        SIZE(COLLECT_SET(city)) AS num_cities,
        COUNT(*) AS event_count
    FROM enriched_carts_2
    GROUP BY CUBE (element, CASE WHEN credit_limit_dense_rank >= 9 THEN 'PREMIUM' ELSE 'STANDARD' END)
)
SELECT element, event_count
FROM grouped
ORDER BY element;
