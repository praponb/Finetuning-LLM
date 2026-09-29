WITH flattened AS (
    SELECT
        id,
        customer_id,
        created_at,
        EXPLODE(coupon_codes) AS code
    FROM carts
),
joined AS (
    SELECT
        a.*,
        b.city,
        b.name,
        b.age
    FROM flattened a
    INNER JOIN customers b
        ON a.customer_id = b.id
),
base AS (
    SELECT
        id,
        customer_id,
        created_at,
        city,
        age,
        FLOOR(age) AS age_whole,
        COALESCE(code, 'N/A') AS resolved_code
    FROM joined
    WHERE city IS NOT NULL
      AND created_at >= TIMESTAMP '2025-03-01 12:00:00'
),
enriched AS (
    SELECT
        customer_id,
        created_at,
        city,
        age,
        age_whole,
        resolved_code,
        AVG(age_whole) OVER (PARTITION BY city ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS age_whole_avg_2h
    FROM base
)
SELECT
    CASE WHEN age_whole >= 73 THEN 'LARGE' ELSE 'SMALL' END AS age_whole_tier,
    CASE WHEN age_whole_avg_2h >= 86 THEN 'HIGH' ELSE 'LOW' END AS age_whole_avg_2h_tier,
    DATE_TRUNC('HOUR', created_at) AS hour_window,
    SIZE(COLLECT_SET(city)) AS num_cities,
    SUM(CASE WHEN city = 'Chiang Mai' THEN age_whole_avg_2h ELSE 0 END) AS chiang_mai_age_whole_avg_2h,
    PERCENTILE_APPROX(age_whole, ARRAY(0.25, 0.5, 0.75)) AS age_whole_quartiles,
    ROUND(AVG(age), 2) AS avg_age
FROM enriched
GROUP BY 1, 2, 3;
