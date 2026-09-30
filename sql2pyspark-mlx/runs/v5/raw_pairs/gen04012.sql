WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        supplier_name,
        country,
        city,
        rating,
        TRIM(supplier_name) AS clean_supplier_name,
        COALESCE(city, 'OTHER') AS city_clean
    FROM suppliers
    WHERE city IS DISTINCT FROM 'Singapore'
      AND supplier_name LIKE '%Part%'
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(rating) OVER (PARTITION BY country) AS country_sum_rating,
        ROUND(rating / SUM(rating) OVER (PARTITION BY city, country), 4) AS rating_share,
        NTILE(10) OVER w_ordered AS rating_bucket,
        AVG(rating) OVER (ORDER BY rating, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS rating_avg_2_prior
    FROM valid
    WINDOW w_ordered AS (PARTITION BY country ORDER BY rating, id)
),
with_latest AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.country,
        s.city,
        s.rating,
        s.clean_supplier_name,
        s.city_clean,
        s.country_sum_rating,
        latest.product_category,
        latest.product_launch_date
    FROM with_windows s
    LEFT JOIN LATERAL (
            SELECT c.category AS product_category, c.launch_date AS product_launch_date
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.cost DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_category ORDER BY country_sum_rating DESC NULLS FIRST, id) AS latest_rank
        FROM with_latest
    ) ranked
    WHERE latest_rank = 1
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('MONTH', product_launch_date) AS activity_month,
    COUNT_IF(city = 'New York') AS new_york_events,
    APPROX_PERCENTILE(rating, 0.95) AS rating_p95,
    APPROX_PERCENTILE(rating, ARRAY(0.25, 0.5, 0.75)) AS rating_quartiles,
    SUM(CASE WHEN city = 'Berlin' THEN country_sum_rating ELSE 0 END) AS berlin_country_sum_rating,
    SUM(rating) AS total_rating,
    SIZE(COLLECT_SET(country)) AS num_countries
FROM deduped
GROUP BY activity_month;
