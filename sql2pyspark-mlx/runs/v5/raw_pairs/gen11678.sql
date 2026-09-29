WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        element
    FROM carts
    LATERAL VIEW EXPLODE(coupon_codes) x AS element
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT *, UPPER(element) AS element_upper
    FROM exploded
    WHERE element IS NOT NULL
      AND customer_id = 29
),
enriched AS (
    -- Attach customers attributes
    SELECT
        src.*,
        p.name,
        p.age,
        p.country
    FROM filtered src
    INNER JOIN customers p
        ON src.customer_id = p.id
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY element ORDER BY age NULLS LAST) AS age_percent_rank,
        PERCENT_RANK() OVER (PARTITION BY customer_id, element ORDER BY age NULLS LAST) AS age_percent_rank_2
    FROM enriched
    WHERE customer_id IN (7, 20, 24, 25)
),
kept_keys AS (
    -- Set difference on keys
    SELECT id, customer_id
    FROM scored
    EXCEPT
    SELECT id, customer_id
    FROM carts
    WHERE TO_DATE(created_at) >= '2024-02-01'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM scored s
    INNER JOIN kept_keys k ON s.id = k.id AND s.customer_id = k.customer_id
)
-- Join a dimension and aggregate
SELECT
    dim.city,
    dim.segment,
    COUNT(*) AS cnt,
    AVG(a.age_percent_rank) AS avg_age_percent_rank
FROM kept_rows a
LEFT JOIN customers dim ON a.customer_id = dim.id
GROUP BY ROLLUP (dim.city, dim.segment)
