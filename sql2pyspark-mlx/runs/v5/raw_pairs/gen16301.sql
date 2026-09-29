WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        supplier_name,
        city,
        rating,
        LENGTH(country) AS country_length,
        CASE WHEN rating >= 4.0 THEN 'High' WHEN rating >= 2.5 THEN 'Medium' ELSE 'Low' END AS rating_tier,
        COALESCE(city, 'OTHER') AS city_clean
    FROM suppliers
    WHERE city IS DISTINCT FROM 'Berlin'
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM cleaned s
    WHERE EXISTS (
            SELECT 1
            FROM products x
            WHERE x.supplier_id = s.id
              AND x.price > 720
        )
),
with_last AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.supplier_name,
        s.city,
        s.rating,
        s.country_length,
        s.rating_tier,
        top1.launch_date
    FROM not_flagged s
    JOIN LATERAL (
            SELECT c.launch_date
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.price DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
with_last_suppliers AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.product_brand
    FROM with_last s
    JOIN LATERAL (
            SELECT c.brand AS product_brand
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.launch_date DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LEAD(rating) OVER (PARTITION BY city ORDER BY rating, id) AS next_rating,
        LAG(rating) OVER (PARTITION BY city ORDER BY rating, id) AS prev_rating,
        LAG(country_length) OVER (PARTITION BY city ORDER BY rating, id) AS prev_country_length,
        LEAD(country_length) OVER (PARTITION BY city ORDER BY rating, id) AS next_country_length,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating, id) AS event_seq
    FROM with_last_suppliers
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products ch
            WHERE ch.supplier_id = s.id
              AND ch.launch_date < s.launch_date
        )
),
enriched_suppliers AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(3) OVER (PARTITION BY product_brand ORDER BY event_seq DESC, id) AS event_seq_bucket,
        DENSE_RANK() OVER (PARTITION BY city ORDER BY rating DESC) AS rating_dense_rank,
        SUM(next_country_length) OVER (PARTITION BY product_brand ORDER BY next_country_length, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_next_country_length
    FROM eligible
)
SELECT
    id,
    supplier_name,
    city,
    country_length,
    launch_date,
    product_brand,
    next_rating,
    next_country_length,
    event_seq_bucket,
    CASE WHEN product_brand RLIKE '^(Initech|Globex)$' THEN 'PRIORITY' WHEN product_brand RLIKE '^(Acme|Umbrella)$' THEN 'CORE' ELSE 'GROUP_A' END AS product_brand_zone
FROM enriched_suppliers
ORDER BY id DESC, next_country_length DESC
