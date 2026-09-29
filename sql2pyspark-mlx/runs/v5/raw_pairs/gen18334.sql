WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        key,
        v
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) x AS key, v
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        a.*,
        d.signup_date,
        d.segment,
        d.age
    FROM flattened a
    LEFT JOIN customers d
        ON a.customer_id = d.id
),
scored AS (
    -- Ranking and gap features
    SELECT *, AVG(age) OVER (PARTITION BY customer_id ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) age_avg_2h
    FROM with_dims
),
with_dims_carts AS (
    -- Attach customers attributes
    SELECT e.*, b.signup_date AS customer_signup_date
    FROM scored e
    INNER JOIN customers b
        ON e.customer_id <=> b.id
),
enriched AS (
    -- Attach customers attributes
    SELECT
        e.*,
        p.city,
        p.country
    FROM with_dims_carts e
    LEFT JOIN customers p
        ON e.customer_id = p.id
)
-- Grouped metrics
SELECT
    DATE_TRUNC('QUARTER', customer_signup_date) AS quarter_start,
    CASE WHEN age_avg_2h >= 61 THEN 'LARGE' ELSE 'SMALL' END AS age_avg_2h_tier,
    COUNT(DISTINCT customer_id) AS distinct_customer_ids,
    COUNT(CASE WHEN city = 'London' THEN 1 END) AS london_count,
    AVG(age_avg_2h) FILTER (WHERE country = 'JP') AS jp_avg_age_avg_2h,
    MIN(created_at) AS started_at,
    SUM(CASE WHEN key = 'gift' THEN age_avg_2h ELSE 0 END) AS gift_age_avg_2h
FROM enriched
GROUP BY quarter_start, age_avg_2h_tier
HAVING SUM(CASE WHEN key = 'gift' THEN age_avg_2h ELSE 0 END) > 0
ORDER BY jp_avg_age_avg_2h NULLS LAST;
