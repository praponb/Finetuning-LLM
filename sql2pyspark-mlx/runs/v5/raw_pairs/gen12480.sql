WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        src.id,
        src.customer_id,
        src.created_at,
        pos + 1 AS line_no,
        elem.product_id,
        elem.qty,
        elem.price,
        elem.promo
    FROM carts src
    LATERAL VIEW OUTER POSEXPLODE(src.items) e AS pos, elem
),
scored AS (
    -- Window metrics over several different partitions
    SELECT *, MAX(price) OVER (PARTITION BY customer_id, promo) AS customer_id_max_price
    FROM flattened
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        s.*,
        p.age,
        p.city
    FROM scored s
    INNER JOIN customers p
        ON s.customer_id = p.id
    WHERE p.signup_date < '2023-06-01'
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        created_at,
        line_no,
        product_id,
        qty,
        promo,
        customer_id_max_price,
        age,
        city,
        CASE WHEN line_no >= 4 THEN 'High' WHEN line_no >= 2 THEN 'Medium' ELSE 'Low' END AS line_no_level,
        line_no - customer_id_max_price AS line_no_minus_customer_id_max_price,
        COALESCE(city, 'DIRECT') AS city_filled
    FROM with_dims
    WHERE created_at > TO_DATE('2025-03-02')
      AND city IS NOT DISTINCT FROM 'Bangkok'
),
base AS (
    -- Basic cleansing and defaults
    SELECT *, line_no_minus_customer_id_max_price - line_no AS line_no_minus_customer_id_max_price_minus_line_no
    FROM valid
    WHERE created_at >= TO_TIMESTAMP('2025-03-02 03:00:00')
      AND customer_id IS NOT NULL
)
-- Join a dimension and aggregate
SELECT
    p.city AS customer_city,
    s.city AS cart_city,
    COUNT(*) AS num_rows,
    MIN(s.line_no_minus_customer_id_max_price) AS min_line_no_minus_customer_id_max_price,
    ROUND(SUM(s.qty), 2) AS total_qty
FROM base s
INNER JOIN customers p ON s.customer_id = p.id
GROUP BY p.city, s.city;
