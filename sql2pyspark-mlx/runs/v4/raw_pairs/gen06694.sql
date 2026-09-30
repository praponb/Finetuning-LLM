WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        pos,
        li_item.qty,
        li_item.price
    FROM carts
    LATERAL VIEW OUTER POSEXPLODE(items) x AS pos, li_item
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        pos,
        qty,
        price,
        PERCENT_RANK() OVER (PARTITION BY customer_id ORDER BY price DESC NULLS FIRST) AS price_percent_rank,
        SUM(pos) OVER (PARTITION BY customer_id ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS pos_sum_1h
    FROM lines
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        src.*,
        p.signup_date,
        p.segment,
        p.age
    FROM with_windows src
    INNER JOIN customers p
        ON src.customer_id = p.id
),
with_dims_carts AS (
    -- Attach customers attributes
    SELECT
        e.*,
        c.name,
        c.signup_date AS customer_signup_date,
        c.country
    FROM with_dims e
    INNER JOIN customers c
        ON e.customer_id = c.id
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        pos,
        qty,
        price,
        price_percent_rank,
        pos_sum_1h,
        signup_date,
        segment,
        age,
        name,
        customer_signup_date,
        country,
        MIN(pos_sum_1h) OVER w_grp AS country_min_pos_sum_1h,
        AVG(price) OVER (PARTITION BY segment ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS price_avg_15m,
        RANK() OVER (ORDER BY price NULLS FIRST) AS price_rank
    FROM with_dims_carts
    WINDOW w_grp AS (PARTITION BY country)
),
matched_keys AS (
    -- Set intersection on keys
    SELECT customer_id
    FROM metrics
    INTERSECT ALL
    SELECT customer_id
    FROM carts
    WHERE HOUR(created_at) BETWEEN 6 AND 13
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM metrics s
    INNER JOIN matched_keys k ON s.customer_id = k.customer_id
),
joined AS (
    -- Attach customers attributes
    SELECT
        src.*,
        b.credit_limit,
        b.email,
        b.age AS customer_age
    FROM kept_rows src
    INNER JOIN customers b
        ON src.customer_id = b.id
),
totals AS (
    -- Multi-level aggregation
    SELECT
        segment,
        AVG(country_min_pos_sum_1h) AS avg_country_min_pos_sum_1h,
        AVG(age) AS avg_age,
        COUNT(*) FILTER (WHERE segment = 'Consumer') AS consumer_rows,
        COUNT(*) AS event_count,
        COUNT(CASE WHEN country = 'UK' THEN 1 END) AS uk_count
    FROM joined
    GROUP BY segment
)
SELECT
    segment,
    avg_country_min_pos_sum_1h,
    avg_age,
    uk_count,
    CASE WHEN segment RLIKE '^(Consumer|Corporate)$' THEN 'EXTENDED' ELSE 'OTHER_ZONE' END AS segment_zone
FROM totals
ORDER BY segment_zone DESC;
