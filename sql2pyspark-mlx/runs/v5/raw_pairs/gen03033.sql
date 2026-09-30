WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        key,
        value
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) x AS key, value
),
exploded AS (
    -- Unnest the array into one row per element
    SELECT
        customer_id,
        key,
        EXPLODE_OUTER(coupon_codes) AS value_2
    FROM lines
),
enriched AS (
    -- Attach customers attributes
    SELECT src.*, c.country
    FROM exploded src
    INNER JOIN customers c
        ON src.customer_id = c.id
),
joined AS (
    -- Attach customers attributes
    SELECT
        src.customer_id,
        src.country,
        d.age,
        d.email,
        d.segment
    FROM enriched src
    INNER JOIN customers d
        ON src.customer_id <=> d.id
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        e.customer_id,
        e.country,
        e.age,
        e.segment,
        d.segment AS customer_segment,
        d.country AS customer_country,
        d.name
    FROM joined e
    INNER JOIN customers d
        ON e.customer_id = d.id
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CONCAT(country, ', ', name) AS country_name,
        CONCAT_WS(',', customer_country, segment) AS customer_country_segment_key,
        COALESCE(country, 'OTHER') AS resolved_country
    FROM with_dims
    WHERE customer_id IS NOT NULL
),
with_dims_carts AS (
    -- Attach customers attributes
    SELECT
        s.customer_id,
        s.country,
        s.age,
        s.segment,
        s.customer_segment,
        s.customer_country,
        s.name,
        s.country_name,
        s.resolved_country,
        b.signup_date
    FROM valid s
    LEFT JOIN customers b
        ON s.customer_id = b.id
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('WEEK', signup_date) AS week_start,
        DATE_TRUNC('MONTH', signup_date) AS activity_month,
        PERCENTILE_APPROX(age, 0.5) AS age_p50,
        SIZE(COLLECT_SET(customer_country)) AS num_customer_countries,
        COUNT(*) FILTER (WHERE customer_country = 'UK') AS uk_rows,
        SUM(age) FILTER (WHERE country = 'JP') AS jp_total_age,
        RANK() OVER (PARTITION BY DATE_TRUNC('WEEK', signup_date) ORDER BY COUNT(*) FILTER (WHERE customer_country = 'UK') DESC) AS uk_rows_rank
    FROM with_dims_carts
    GROUP BY 1, 2
)
SELECT
    week_start,
    activity_month,
    num_customer_countries,
    jp_total_age
FROM totals
ORDER BY week_start NULLS FIRST, activity_month;
