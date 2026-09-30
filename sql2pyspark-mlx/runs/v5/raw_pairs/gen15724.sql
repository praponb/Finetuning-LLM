WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        supplier_name,
        country,
        city,
        rating,
        CASE WHEN rating >= 3.9 THEN 'High' WHEN rating >= 1.9 THEN 'Medium' ELSE 'Low' END AS rating_band,
        COALESCE(city, 'UNKNOWN') AS city_filled
    FROM suppliers
    WHERE city IS NOT NULL
),
valid_suppliers AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        TRIM(country) AS clean_country,
        CASE WHEN rating >= 3.8 THEN 'High' WHEN rating >= 2.3 THEN 'Medium' ELSE 'Low' END AS rating_tier
    FROM valid
    WHERE city IN ('Bangkok', 'Berlin')
),
latest_match AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.country,
        s.rating_band,
        s.city_filled,
        s.clean_country,
        s.rating_tier,
        x.price
    FROM valid_suppliers s
    LEFT JOIN LATERAL (
            SELECT c.price
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.cost DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, ROUND(price * 0.85, 2) AS adjusted_price
    FROM latest_match
    WHERE country = 'TH'
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        country,
        rating_band,
        rating_tier,
        price,
        adjusted_price,
        CASE WHEN country = 'SG' THEN 1 ELSE 0 END AS is_sg
    FROM cleaned
    WHERE adjusted_price BETWEEN 56 AND 71
),
with_last AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.rating_band,
        s.adjusted_price,
        s.is_sg,
        latest.launch_date
    FROM filtered s
    JOIN LATERAL (
            SELECT c.launch_date
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.cost DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        rating_band,
        adjusted_price,
        is_sg,
        launch_date,
        MAX(adjusted_price) OVER w_part AS last3_max_adjusted_price,
        COUNT(adjusted_price) OVER w_part AS last3_count_adjusted_price,
        SUM(adjusted_price) OVER w_part AS last3_sum_adjusted_price,
        NTILE(3) OVER (ORDER BY adjusted_price, id) AS adjusted_price_bucket
    FROM with_last
    WHERE adjusted_price >= 60
    WINDOW w_part AS (ORDER BY adjusted_price, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
)
-- Multi-level aggregation
SELECT
    CASE WHEN last3_count_adjusted_price >= 15 THEN 'LARGE' ELSE 'SMALL' END AS last3_count_adjusted_price_tier,
    APPROX_PERCENTILE(last3_max_adjusted_price, 0.5) AS last3_max_adjusted_price_p50,
    PERCENTILE_APPROX(adjusted_price_bucket, 0.9) AS adjusted_price_bucket_p90,
    ROUND(AVG(adjusted_price), 2) AS avg_adjusted_price
FROM metrics
GROUP BY last3_count_adjusted_price_tier
ORDER BY last3_max_adjusted_price_p50 DESC NULLS FIRST
