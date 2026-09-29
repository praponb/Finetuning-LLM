WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        code
    FROM carts
    LATERAL VIEW OUTER EXPLODE(coupon_codes) x AS code
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        a.id,
        a.customer_id,
        c.signup_date,
        c.country,
        c.city
    FROM lines a
    LEFT JOIN customers c
        ON a.customer_id = c.id
),
joined AS (
    -- Attach customers attributes
    SELECT
        src.customer_id,
        src.signup_date,
        src.country,
        p.city AS customer_city
    FROM with_dims src
    LEFT JOIN customers p
        ON src.customer_id = p.id
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, LAST_DAY(signup_date) AS signup_date_month_end
    FROM joined
    WHERE customer_city IS DISTINCT FROM 'Bangkok'
),
enriched AS (
    -- Attach customers attributes
    SELECT
        s.customer_id,
        s.signup_date,
        s.customer_city,
        b.segment
    FROM valid s
    LEFT JOIN customers b
        ON s.customer_id = b.id
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        customer_id,
        SORT_ARRAY(COLLECT_SET(customer_city)) AS customer_city_list,
        SIZE(COLLECT_SET(segment)) AS num_segments,
        COUNT(*) AS event_count,
        COUNT(*) FILTER (WHERE segment = 'Corporate') AS corporate_rows,
        SORT_ARRAY(COLLECT_SET(customer_city)) AS customer_city_list_2,
        SORT_ARRAY(COLLECT_SET(customer_city)) AS customer_city_list_3
    FROM enriched
    GROUP BY CUBE (customer_id)
)
SELECT
    customer_id,
    customer_city_list,
    event_count,
    corporate_rows,
    customer_city_list_2
FROM totals
ORDER BY event_count DESC NULLS FIRST;
