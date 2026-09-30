WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        line.product_id,
        line.qty,
        line.price,
        line.promo
    FROM (
        SELECT
            id,
            customer_id,
            created_at,
            coupon_codes,
            EXPLODE_OUTER(items) AS line
        FROM carts
    ) x
),
flattened_carts AS (
    -- Unnest the array into one row per element
    SELECT
        customer_id,
        created_at,
        product_id,
        qty,
        price,
        element
    FROM flattened
    LATERAL VIEW EXPLODE(coupon_codes) t AS element
),
enriched AS (
    -- Attach customers attributes
    SELECT
        e.*,
        d.credit_limit,
        d.age
    FROM flattened_carts e
    LEFT JOIN customers d
        ON e.customer_id = d.id
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, price * credit_limit AS price_times_credit_limit
    FROM enriched
    WHERE created_at >= TO_TIMESTAMP('2025-03-01 20:00:00')
      AND element IS NOT NULL
      AND age IS NOT NULL
),
joined AS (
    -- Attach customers attributes
    SELECT
        e.*,
        b.credit_limit AS customer_credit_limit,
        b.age AS customer_age
    FROM cleaned e
    LEFT JOIN customers b
        ON e.customer_id = b.id
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        customer_id,
        created_at,
        product_id,
        qty,
        price,
        element,
        credit_limit,
        age,
        price_times_credit_limit,
        customer_credit_limit,
        customer_age,
        ROUND(customer_credit_limit / SUM(customer_credit_limit) OVER (PARTITION BY customer_id, element), 4) AS customer_credit_limit_share,
        COUNT(age) OVER (PARTITION BY element, customer_id ORDER BY CAST(created_at AS BIGINT) ASC RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS age_count_15m,
        ROUND(price / SUM(price) OVER (PARTITION BY element), 4) AS price_share
    FROM joined
)
SELECT
    s.customer_id,
    s.created_at,
    s.product_id,
    s.qty,
    s.element,
    s.credit_limit,
    s.price_times_credit_limit,
    s.customer_age,
    s.customer_credit_limit_share,
    (SELECT AVG(t2.balance) FROM accounts t2 WHERE t2.customer_id = s.customer_id) AS avg_account_balance
FROM with_windows s
ORDER BY credit_limit ASC NULLS LAST, customer_credit_limit_share ASC;
