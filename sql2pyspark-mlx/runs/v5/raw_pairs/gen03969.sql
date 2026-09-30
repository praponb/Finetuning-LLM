WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        attr_key,
        value
    FROM carts
    LATERAL VIEW EXPLODE(attributes) li AS attr_key, value
),
scored AS (
    -- Running and moving aggregates
    SELECT *, LAST_VALUE(attr_key) IGNORE NULLS OVER (PARTITION BY attr_key, customer_id ORDER BY created_at, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_attr_key
    FROM flattened
),
joined AS (
    -- Attach customers attributes
    SELECT
        src.*,
        c.age,
        c.signup_date
    FROM scored src
    JOIN customers c
        ON src.customer_id = c.id
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        customer_id,
        attr_key,
        value,
        signup_date,
        AVG(age) OVER (PARTITION BY customer_id, attr_key ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS age_avg_15m,
        SUM(age) OVER (PARTITION BY customer_id) AS customer_id_sum_age,
        LAG(age, 1) OVER (PARTITION BY customer_id ORDER BY created_at, id, attr_key) AS prev_age
    FROM joined
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY customer_id ORDER BY customer_id_sum_age DESC) AS latest_rank
        FROM enriched
    ) x
    WHERE latest_rank = 1
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN age_avg_15m >= 74 THEN 'HIGH' ELSE 'LOW' END AS age_avg_15m_tier,
    customer_id,
    SUM(CASE WHEN attr_key = 'gift' THEN customer_id_sum_age ELSE 0 END) AS gift_customer_id_sum_age,
    PERCENTILE_APPROX(customer_id_sum_age, ARRAY(0.25, 0.5, 0.75)) AS customer_id_sum_age_quartiles,
    SUM(CASE WHEN attr_key = 'device' THEN customer_id_sum_age ELSE 0 END) AS device_customer_id_sum_age
FROM latest
GROUP BY ROLLUP (CASE WHEN age_avg_15m >= 74 THEN 'HIGH' ELSE 'LOW' END, customer_id)
ORDER BY age_avg_15m_tier DESC NULLS LAST
