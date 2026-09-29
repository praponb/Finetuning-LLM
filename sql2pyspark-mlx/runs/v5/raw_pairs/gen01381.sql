WITH exploded AS (
    SELECT
        id,
        customer_id,
        created_at,
        value
    FROM carts
    LATERAL VIEW OUTER EXPLODE(coupon_codes) e AS value
),
with_dims AS (
    SELECT
        s.customer_id,
        s.created_at,
        p.age
    FROM exploded s
    LEFT JOIN customers p
        ON s.customer_id = p.id
    WHERE p.name LIKE '%Kanya%'
),
cleaned AS (
    SELECT *, CAST(age AS DOUBLE) AS age_num
    FROM with_dims
    WHERE customer_id IN (20, 21, 39)
),
with_dims_carts AS (
    SELECT
        s.customer_id,
        s.created_at,
        c.segment
    FROM cleaned s
    LEFT JOIN customers c
        ON s.customer_id = c.id
),
filtered AS (
    SELECT
        *,
        DAYOFWEEK(created_at) AS created_at_dayofweek,
        CAST(created_at AS DATE) AS created_at_day,
        COALESCE(segment, 'OTHER') AS resolved_segment
    FROM with_dims_carts
    WHERE segment IS NOT NULL
),
enriched AS (
    SELECT
        e.*,
        d.name,
        d.signup_date,
        d.credit_limit
    FROM filtered e
    INNER JOIN customers d
        ON e.customer_id = d.id
),
enriched_carts AS (
    SELECT
        created_at_day,
        resolved_segment,
        name,
        signup_date,
        credit_limit,
        MAX(credit_limit) OVER (ORDER BY CAST(created_at AS BIGINT) ASC RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS credit_limit_max_2h
    FROM enriched
)
SELECT
    DATE_TRUNC('WEEK', signup_date) AS week_start,
    SUM(credit_limit_max_2h) AS total_credit_limit_max_2h,
    ROUND(AVG(credit_limit_max_2h), 2) AS avg_credit_limit_max_2h,
    ROUND(AVG(credit_limit), 2) AS avg_credit_limit
FROM enriched_carts
GROUP BY DATE_TRUNC('WEEK', signup_date)
ORDER BY total_credit_limit_max_2h ASC
