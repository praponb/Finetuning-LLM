WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CONCAT_WS('-', country, city) AS country_city_key,
        COALESCE(city, 'DIRECT') AS resolved_city
    FROM suppliers
    WHERE city IS NOT NULL
),
valid_suppliers AS (
    -- Keep recent, valid rows
    SELECT
        id,
        supplier_name,
        country,
        city,
        rating,
        country_city_key,
        resolved_city,
        LENGTH(supplier_name) AS supplier_name_length
    FROM valid
    WHERE city = 'London'
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        country,
        rating,
        country_city_key,
        resolved_city,
        supplier_name_length
    FROM valid_suppliers
    QUALIFY ROW_NUMBER() OVER (PARTITION BY country, city ORDER BY supplier_name_length DESC NULLS LAST, id) = 1
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        rating,
        PERCENT_RANK() OVER w_grp AS supplier_name_length_percent_rank,
        FIRST_VALUE(rating, TRUE) OVER w_ordered AS first_known_rating,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY supplier_name_length DESC, id) AS event_seq
    FROM latest
    WHERE rating > 2.5
    WINDOW w_grp AS (PARTITION BY country ORDER BY supplier_name_length DESC), w_ordered AS (PARTITION BY country ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_last AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.category,
        x.product_cost
    FROM metrics s
    JOIN LATERAL (
            SELECT c.category, c.cost AS product_cost
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.price DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
-- Multi-level aggregation
SELECT
    CASE WHEN product_cost >= 710 THEN 'PREMIUM' ELSE 'STANDARD' END AS product_cost_tier,
    CASE WHEN rating >= 4.1 THEN 'LARGE' ELSE 'SMALL' END AS rating_tier,
    ROUND(SUM(first_known_rating) / NULLIF(COUNT(CASE WHEN category = 'Grocery' THEN 1 END), 0), 2) AS first_known_rating_per_grocery,
    COUNT(DISTINCT category) AS distinct_categories,
    COUNT(CASE WHEN category = 'Sports' THEN 1 END) AS sports_count
FROM with_last
GROUP BY product_cost_tier, rating_tier
